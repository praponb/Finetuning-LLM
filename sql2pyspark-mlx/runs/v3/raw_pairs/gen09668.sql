SELECT status
FROM orders
WHERE order_date BETWEEN '2025-02-01' AND '2025-03-01'
UNION ALL
SELECT status
FROM orders
WHERE amount > 770
ORDER BY status;
