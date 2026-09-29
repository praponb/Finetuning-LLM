SELECT s.device, COUNT(CASE WHEN we.page = '/home' THEN 1 END) home_count
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
GROUP BY s.device
ORDER BY home_count DESC
