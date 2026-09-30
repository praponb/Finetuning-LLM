select we.page, we.duration_sec, s.started_at, round(we.duration_sec * 1.15, 2) as duration_sec_scaled from web_events we inner join sessions s on we.session_id = s.id where we.page = '/home'
