SELECT channel
FROM orders
EXCEPT
SELECT channel
FROM orders
WHERE order_date BETWEEN '2025-01-09' AND '2025-04-01'
