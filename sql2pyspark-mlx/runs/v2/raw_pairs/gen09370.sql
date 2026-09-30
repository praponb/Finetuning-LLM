SELECT event_type, duration_sec, page, DATE_TRUNC('year', event_time) AS event_time_period FROM web_events
