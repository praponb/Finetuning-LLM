SELECT status
FROM orders
WHERE order_date BETWEEN '2023-03-01' AND '2023-12-01'
UNION
SELECT status
FROM shipments
WHERE shipped_date < '2025-04-01';
