select s.id, s.country, s.started_at, c.country as customer_country from sessions s full outer join customers c on c.id = s.customer_id
