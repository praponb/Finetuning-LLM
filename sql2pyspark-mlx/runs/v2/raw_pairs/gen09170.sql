SELECT s.country, s.device, MIN(we.duration_sec) AS min_duration_sec FROM web_events we LEFT JOIN sessions s ON we.session_id = s.id WHERE s.started_at < '2023-08-01' GROUP BY s.country, s.device;
