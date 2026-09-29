SELECT we.id, we.event_type, s.device FROM web_events we INNER JOIN sessions s ON we.session_id = s.id WHERE we.duration_sec > 454;
