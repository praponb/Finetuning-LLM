select s.started_at, c.signup_date, c.city from sessions s inner join customers c on c.id = s.customer_id;
