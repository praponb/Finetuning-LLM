select s.started_at, s.country, c.name, c.city, c.segment from sessions s left join customers c on s.customer_id = c.id
