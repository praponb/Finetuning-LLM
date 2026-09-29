SELECT p.id, p.status, o.store_id, o.status order_status, c.email, c.segment FROM payments p LEFT JOIN orders o ON o.id = p.order_id INNER JOIN customers c ON o.customer_id = c.id ORDER BY p.id DESC
