SELECT event_type, page, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range
FROM web_events
GROUP BY 1, 2;
