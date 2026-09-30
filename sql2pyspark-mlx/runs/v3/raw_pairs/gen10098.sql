select s.id, s.device, s.started_at, c.signup_date from sessions s inner join customers c on c.id = s.customer_id
