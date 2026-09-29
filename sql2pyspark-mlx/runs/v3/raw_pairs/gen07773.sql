SELECT signup_date
FROM customers
UNION ALL
SELECT order_date
FROM orders
WHERE discount > 0.33
ORDER BY signup_date
