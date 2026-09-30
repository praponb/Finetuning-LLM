SELECT s.browser, COUNT(we.id) AS web_event_count FROM sessions s INNER JOIN web_events we ON we.session_id = s.id WHERE we.duration_sec BETWEEN 194 AND 251 GROUP BY s.browser ORDER BY s.browser;
