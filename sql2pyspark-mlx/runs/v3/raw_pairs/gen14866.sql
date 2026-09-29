SELECT we.duration_sec, we.page, s.device FROM web_events we INNER JOIN sessions s ON s.id = we.session_id WHERE s.started_at < '2023-10-01';
