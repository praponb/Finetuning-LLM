SELECT p.id, p.method, o.discount, c.country, c.city FROM payments p INNER JOIN orders o ON o.id = p.order_id INNER JOIN customers c ON o.customer_id = c.id WHERE p.paid_at < '2025-01-01';
