SELECT MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range, SUM(duration_sec) AS total_duration_sec
FROM web_events
WHERE page = '/product'
ORDER BY duration_sec_range DESC;
