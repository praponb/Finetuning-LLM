SELECT c.id, c.credit_limit, c.country FROM customers c WHERE EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id AND s.customer_id IN (3, 5, 12)) ORDER BY country DESC;
