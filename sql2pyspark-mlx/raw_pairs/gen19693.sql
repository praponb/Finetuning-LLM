SELECT signup_date, 'customer' AS source
FROM customers
UNION ALL
SELECT order_date, 'order' AS source
FROM orders
WHERE status = 'SHIPPED'
