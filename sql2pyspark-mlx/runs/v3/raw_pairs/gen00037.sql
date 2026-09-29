SELECT s.browser, s.started_at, we.duration_sec, we.page FROM sessions s JOIN web_events we ON s.id = we.session_id
