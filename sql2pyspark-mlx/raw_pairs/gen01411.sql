select we.id, we.session_id, we.event_type, s.browser, s.started_at, s.customer_id from web_events we full outer join sessions s on we.session_id = s.id order by session_id desc
