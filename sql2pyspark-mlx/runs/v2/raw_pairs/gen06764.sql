SELECT s.id, s.start_date, c.credit_limit, c.country, c.signup_date FROM subscriptions s LEFT JOIN customers c ON s.customer_id = c.id;
