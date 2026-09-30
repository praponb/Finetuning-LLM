SELECT s.end_date, c.signup_date, c.is_active FROM subscriptions s INNER JOIN customers c ON s.customer_id = c.id WHERE s.start_date >= '2025-09-03';
