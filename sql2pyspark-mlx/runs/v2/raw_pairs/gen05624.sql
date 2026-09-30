SELECT s.browser, we.duration_sec, we.page FROM sessions s LEFT JOIN web_events we ON we.session_id = s.id ORDER BY page DESC, duration_sec DESC;
