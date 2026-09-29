SELECT we.id, we.page, s.country, s.started_at, TRIM(s.device) AS clean_device FROM web_events we INNER JOIN sessions s ON we.session_id = s.id ORDER BY id;
