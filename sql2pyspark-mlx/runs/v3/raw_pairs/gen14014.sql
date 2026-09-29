select c.credit_limit, c.city, c.age, s.browser, s.customer_id, s.started_at from customers c inner join sessions s on c.id = s.customer_id order by city desc
