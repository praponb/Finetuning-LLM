SELECT email, 'customer' AS kind
FROM customers
UNION ALL
SELECT channel, 'order' AS kind
FROM orders
ORDER BY email
