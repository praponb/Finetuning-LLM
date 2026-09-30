select s.id, s.started_at, c.country, c.name from sessions s left outer join customers c on s.customer_id = c.id
