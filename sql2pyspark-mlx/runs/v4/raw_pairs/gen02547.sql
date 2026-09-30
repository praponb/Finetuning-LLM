select we.id, we.event_time, s.customer_id, s.started_at from web_events we inner join sessions s on we.session_id = s.id order by customer_id
