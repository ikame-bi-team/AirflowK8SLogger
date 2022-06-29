import logging
import sys

from file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin


class KubernetesTaskHandler(FileTaskHandler, LoggingMixin):
    """
    Supplementary handler, writes log into stdout, this allows FileTaskHandler to read
    K8S pod log from stdout using K8S api.
    """

    def __init__(self, base_log_folder, filename_template):
        """
        :param base_log_folder: base folder to store logs locally
        :param log_id_template: log id template
        """
        super(KubernetesTaskHandler, self).__init__(
            base_log_folder, filename_template)
        self.closed = False

        self.mark_end_on_close = True
        self.handler = None
        self.context_set = False

    def set_context(self, ti):
        """
        Provide task_instance context to airflow task handler.
        :param ti: task instance object
        """
        self.mark_end_on_close = not ti.raw

        if self.context_set:
            # We don't want to re-set up the handler if this logger has
            # already been initialized
            return

        self.handler = logging.StreamHandler(stream=sys.__stdout__)
        self.handler.setLevel(self.level)
        self.handler.setFormatter(self.formatter)

        self.context_set = True

    def close(self):
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return

        if not self.mark_end_on_close:
            self.closed = True
            return

        # Case which context of the handler was not set.
        if self.handler is None:
            self.closed = True
            return

        # Reopen the file stream, because FileHandler.close() would be called
        # first in logging.shutdown() and the stream in it would be set to None.
        if self.handler.stream is None or self.handler.stream.closed:
            self.handler.stream = self.handler._open()

        self.handler.close()
        sys.stdout = sys.__stdout__

        super(KubernetesTaskHandler, self).close()

        self.closed = True