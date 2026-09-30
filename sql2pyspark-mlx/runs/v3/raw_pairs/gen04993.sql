SELECT s.id, s.country, COUNT(we.id) AS web_event_count FROM sessions s LEFT OUTER JOIN web_events we ON we.session_id = s.id GROUP BY s.id, s.country ORDER BY web_event_count DESC
