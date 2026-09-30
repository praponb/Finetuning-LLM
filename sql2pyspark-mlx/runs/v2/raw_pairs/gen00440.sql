SELECT we.duration_sec, we.page, s.device FROM web_events we INNER JOIN sessions s ON s.id = we.session_id WHERE s.started_at >= '2025-08-09' AND we.page = '/checkout';
