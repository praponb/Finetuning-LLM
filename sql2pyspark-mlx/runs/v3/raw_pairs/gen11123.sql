select s.id, s.customer_id, c.signup_date, c.credit_limit, c.name from subscriptions s left outer join customers c on s.customer_id = c.id order by signup_date asc, s.id desc limit 5;
