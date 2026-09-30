SELECT s.plan, s.end_date, c.signup_date, c.age, c.is_active FROM subscriptions s INNER JOIN customers c ON s.customer_id = c.id WHERE c.age > 63;
