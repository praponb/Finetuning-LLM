SELECT delivered_date
FROM shipments
WHERE shipped_date >= '2023-11-01'
EXCEPT
SELECT opened_date
FROM stores
WHERE city = 'Singapore'
