SELECT DATE_TRUNC('MONTH', event_time) event_month, page, MAX(duration_sec) - MIN(duration_sec) duration_sec_range
FROM web_events
WHERE duration_sec BETWEEN 202 AND 304
GROUP BY 1, 2
HAVING MAX(duration_sec) - MIN(duration_sec) < 47;
