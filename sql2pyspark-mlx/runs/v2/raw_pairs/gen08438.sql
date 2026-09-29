SELECT o.id, o.discount
FROM orders o
WHERE o.order_date >= '2025-09-01' AND o.customer_id IN (SELECT id FROM customers WHERE age IS NOT NULL)
