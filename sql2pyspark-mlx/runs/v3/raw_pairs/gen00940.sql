SELECT we.session_id, we.event_type, s.device FROM web_events we JOIN sessions s ON we.session_id = s.id WHERE we.event_type = 'page_view'
