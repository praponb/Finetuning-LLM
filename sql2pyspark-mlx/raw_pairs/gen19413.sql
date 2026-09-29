SELECT *, CASE WHEN duration_sec >= 503 THEN 'High' WHEN duration_sec >= 224 THEN 'Medium' ELSE 'Low' END AS duration_sec_level, DATEDIFF(CURRENT_DATE(), event_time) / 365.25 AS years_since_event_time
FROM web_events
WHERE page = '/checkout'
ORDER BY id DESC, session_id DESC
LIMIT 5;
