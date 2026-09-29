SELECT s.id, s.end_date, c.credit_limit FROM subscriptions s INNER JOIN customers c ON s.customer_id = c.id ORDER BY id LIMIT 5
