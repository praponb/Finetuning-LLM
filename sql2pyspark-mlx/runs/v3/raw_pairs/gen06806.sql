SELECT id, event_time, DATE_TRUNC('WEEK', event_time) AS event_time_period FROM web_events WHERE duration_sec > 460 AND HOUR(event_time) BETWEEN 11 AND 15
