SELECT s.started_at, we.session_id FROM sessions s LEFT JOIN web_events we ON we.session_id = s.id WHERE s.started_at >= '2024-08-11' AND we.page NOT IN ('/cart', '/checkout') ORDER BY s.started_at;
