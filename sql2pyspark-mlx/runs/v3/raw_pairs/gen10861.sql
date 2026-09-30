SELECT s.browser, MAX(we.event_time) AS last_event_time FROM web_events we INNER JOIN sessions s ON we.session_id = s.id GROUP BY s.browser
