SELECT DATE_TRUNC('MONTH', event_time) AS event_month, MAX(duration_sec) AS max_duration_sec, SUM(duration_sec) AS total_duration_sec
FROM web_events
GROUP BY DATE_TRUNC('MONTH', event_time)
