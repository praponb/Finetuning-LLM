SELECT s.id, s.browser, COUNT(we.id) AS web_event_count FROM sessions s LEFT JOIN web_events we ON we.session_id = s.id GROUP BY s.id, s.browser ORDER BY id DESC, browser LIMIT 1
