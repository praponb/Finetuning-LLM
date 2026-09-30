SELECT s.end_date, c.signup_date, c.email FROM subscriptions s LEFT JOIN customers c ON s.customer_id = c.id WHERE s.monthly_fee BETWEEN 90 AND 200
