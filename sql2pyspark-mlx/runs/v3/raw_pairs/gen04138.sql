select we.duration_sec, we.page, we.session_id, s.started_at, s.device, date_trunc('MONTH', s.started_at) started_at_period from web_events we full outer join sessions s on we.session_id = s.id
