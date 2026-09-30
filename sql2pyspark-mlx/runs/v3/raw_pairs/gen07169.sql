SELECT session_id, CASE WHEN duration_sec >= 382 THEN 'High' WHEN duration_sec >= 97 THEN 'Medium' ELSE 'Low' END AS duration_sec_band, ROUND(duration_sec * 1.15, 2) AS adjusted_duration_sec
FROM web_events
WHERE page = '/checkout' AND duration_sec BETWEEN 237 AND 352;
