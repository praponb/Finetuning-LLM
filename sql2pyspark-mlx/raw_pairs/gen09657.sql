select we.id, we.event_type, we.session_id, s.device, c.age from web_events we left join sessions s on we.session_id = s.id left join customers c on c.id = s.customer_id order by age asc
