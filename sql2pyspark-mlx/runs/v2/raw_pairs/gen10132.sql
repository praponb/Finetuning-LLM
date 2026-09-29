SELECT HOUR(s.started_at) AS started_hour, s.country, COUNT(*) AS cnt FROM web_events we LEFT OUTER JOIN sessions s ON we.session_id = s.id GROUP BY HOUR(s.started_at), s.country HAVING COUNT(*) > 5
