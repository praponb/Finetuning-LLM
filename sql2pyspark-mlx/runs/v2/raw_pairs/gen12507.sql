SELECT status
FROM orders
UNION ALL
SELECT status
FROM shipments
WHERE delivered_date >= '2023-03-01'
