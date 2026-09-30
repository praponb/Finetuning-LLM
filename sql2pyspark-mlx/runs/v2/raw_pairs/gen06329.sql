SELECT session_id, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range, SUM(duration_sec) AS total_duration_sec
FROM web_events
GROUP BY 1
HAVING total_duration_sec > 1700
ORDER BY duration_sec_range DESC, total_duration_sec DESC, session_id
LIMIT 10;
