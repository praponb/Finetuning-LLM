SELECT we.event_type, s.started_at, s.country FROM web_events we LEFT OUTER JOIN sessions s ON we.session_id = s.id
