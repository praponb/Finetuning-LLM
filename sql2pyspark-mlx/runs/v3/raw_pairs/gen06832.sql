SELECT we.id, we.event_time, we.session_id, we.duration_sec, s.country, s.browser FROM web_events we JOIN sessions s ON we.session_id = s.id WHERE s.started_at < '2023-12-01'
