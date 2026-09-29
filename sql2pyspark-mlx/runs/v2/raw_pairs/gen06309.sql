SELECT s.customer_id, s.plan, s.status, c.age FROM subscriptions s INNER JOIN customers c ON s.customer_id = c.id ORDER BY age DESC
