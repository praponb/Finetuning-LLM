SELECT s.browser, COUNT(we.id) web_event_count FROM sessions s INNER JOIN web_events we ON s.id = we.session_id GROUP BY s.browser
