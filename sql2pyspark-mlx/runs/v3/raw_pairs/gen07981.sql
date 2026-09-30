SELECT s.device, we.duration_sec, we.page
FROM sessions s
LEFT JOIN web_events we ON s.id = we.session_id
ORDER BY page
