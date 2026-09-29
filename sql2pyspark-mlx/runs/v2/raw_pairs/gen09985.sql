select we.session_id, s.device from web_events we left outer join sessions s on we.session_id = s.id where hour(s.started_at) between 8 and 13
