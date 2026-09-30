SELECT s.id, s.start_date, c.age, c.email FROM subscriptions s INNER JOIN customers c ON s.customer_id = c.id WHERE s.plan IN ('premium', 'enterprise', 'basic') ORDER BY age DESC
