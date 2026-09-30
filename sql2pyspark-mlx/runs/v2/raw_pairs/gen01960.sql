select s.id, s.device, s.country, s.started_at, c.signup_date, c.credit_limit from sessions s left join customers c on s.customer_id = c.id;
