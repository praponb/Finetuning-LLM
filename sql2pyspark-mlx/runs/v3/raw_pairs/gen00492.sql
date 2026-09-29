SELECT s.id, s.start_date, s.end_date, s.status, c.city, c.signup_date FROM subscriptions s LEFT OUTER JOIN customers c ON s.customer_id = c.id
