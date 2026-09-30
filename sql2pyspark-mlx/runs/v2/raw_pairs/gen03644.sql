SELECT MIN(duration_sec) AS min_duration_sec, ROUND((100 * SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_add_to_cart, AVG(duration_sec) AS avg_duration_sec
FROM web_events
WHERE duration_sec != 151
