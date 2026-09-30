select s.country, c.is_active from sessions s inner join customers c on c.id = s.customer_id where not (c.country = 'SG' or s.started_at < '2024-01-01') order by country
