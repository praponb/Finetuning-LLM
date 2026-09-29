SELECT s.id, s.end_date, s.monthly_fee, s.status, c.segment, c.signup_date, c.age FROM subscriptions s LEFT JOIN customers c ON s.customer_id = c.id;
