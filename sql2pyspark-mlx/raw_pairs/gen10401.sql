SELECT s.id, s.order_id, o.order_date, o.customer_id, c.email, c.credit_limit FROM shipments s JOIN orders o ON s.order_id = o.id JOIN customers c ON o.customer_id = c.id
