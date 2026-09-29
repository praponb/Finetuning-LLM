SELECT s.id, COUNT(we.id) AS web_event_count, COUNT(we.duration_sec) AS duration_sec_count
FROM sessions s
LEFT OUTER JOIN web_events we ON s.id = we.session_id
WHERE we.event_time < '2025-04-01'
GROUP BY s.id
ORDER BY web_event_count DESC;
