select s.country, s.started_at, c.country as customer_country, c.email from sessions s join customers c on s.customer_id = c.id order by email
