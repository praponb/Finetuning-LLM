select we.id, we.event_type, s.started_at, c.city, c.email from web_events we inner join sessions s on s.id = we.session_id inner join customers c on s.customer_id = c.id;
