SELECT s.order_id, o.amount, o.customer_id, c.credit_limit, c.segment FROM shipments s LEFT OUTER JOIN orders o ON o.id = s.order_id JOIN customers c ON o.customer_id = c.id
