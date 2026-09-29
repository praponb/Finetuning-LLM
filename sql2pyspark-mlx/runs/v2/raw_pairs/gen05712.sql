SELECT sessions.device, COUNT(DISTINCT web_events.duration_sec) unique_sessions, SUM(CASE WHEN web_events.page = '/search' THEN 1 ELSE 0 END) search_count
FROM web_events
RIGHT OUTER JOIN sessions ON sessions.id = web_events.session_id
JOIN customers ON sessions.customer_id = customers.id
GROUP BY sessions.device
ORDER BY sessions.device ASC;
