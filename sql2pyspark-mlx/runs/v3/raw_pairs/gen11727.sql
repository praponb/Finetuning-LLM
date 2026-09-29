SELECT p.id, p.amount, o.status, c.is_active, c.email FROM payments p LEFT OUTER JOIN orders o ON p.order_id = o.id RIGHT OUTER JOIN customers c ON o.customer_id = c.id WHERE o.store_id IS NOT NULL
