SELECT s.order_id, s.status, o.customer_id, o.discount FROM shipments s LEFT JOIN orders o ON s.order_id = o.id WHERE s.carrier IS NOT NULL ORDER BY customer_id DESC
