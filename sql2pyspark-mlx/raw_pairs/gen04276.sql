SELECT we.event_type, s.country FROM web_events we INNER JOIN sessions s ON we.session_id = s.id WHERE s.device = 'desktop'
