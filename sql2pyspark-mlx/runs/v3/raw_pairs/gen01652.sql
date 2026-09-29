SELECT *, DATE_TRUNC('month', event_time) AS event_time_period, SUBSTRING(page, 1, 4) AS page_prefix
FROM web_events
WHERE duration_sec <= 226
ORDER BY duration_sec ASC, id ASC
LIMIT 5;
