SELECT s.id, s.customer_id, c.name, c.is_active FROM subscriptions s INNER JOIN customers c ON s.customer_id = c.id;
