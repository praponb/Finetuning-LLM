SELECT s.started_at, we.event_type FROM sessions s LEFT JOIN web_events we ON s.id = we.session_id WHERE we.page IS NULL
