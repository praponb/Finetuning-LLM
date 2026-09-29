SELECT c.id, c.is_active, c.email
FROM customers c
WHERE c.email NOT LIKE '%@outlook.com' AND c.id IN (SELECT customer_id FROM orders WHERE order_date <= '2025-09-01')
ORDER BY email DESC, id
