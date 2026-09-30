SELECT we.event_type, we.event_time, s.country, s.device FROM web_events we JOIN sessions s ON we.session_id = s.id;
