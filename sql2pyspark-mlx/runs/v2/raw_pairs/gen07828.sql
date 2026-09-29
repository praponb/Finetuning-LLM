SELECT o.id, o.order_date
FROM orders o
WHERE o.order_date <= '2025-05-01' AND o.customer_id IN (SELECT id FROM customers WHERE segment IN ('SMB', 'Corporate', 'Consumer'))
