select s.id, s.device, c.name, c.credit_limit - c.age as credit_limit_minus_age from sessions s join customers c on c.id = s.customer_id where s.customer_id = 39;
