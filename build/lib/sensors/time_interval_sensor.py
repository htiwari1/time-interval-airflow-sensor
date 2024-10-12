# sensors/time_interval_sensor.py
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, time
import pytz

class TimeIntervalSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, start_time: time, end_time: time, time_zone: str = 'UTC', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_time = start_time
        self.end_time = end_time
        self.time_zone = time_zone

    def poke(self, context):
        # Get the current time in the specified time zone
        tz = pytz.timezone(self.time_zone)
        now = datetime.now(tz).time()

        # Convert start_time and end_time to the time zone
        start_time_tz = tz.localize(datetime.combine(datetime.now(), self.start_time)).time()
        end_time_tz = tz.localize(datetime.combine(datetime.now(), self.end_time)).time()

        # Check if the current time is within the interval
        return start_time_tz <= now <= end_time_tz
