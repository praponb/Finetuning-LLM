select c.id, c.name, c.signup_date, c.age from customers c where not exists (select 1 from sessions s where s.customer_id = c.id and to_date(s.started_at) < '2023-08-01');
