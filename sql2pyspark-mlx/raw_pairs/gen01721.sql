SELECT status, order_id
FROM shipments
WHERE shipped_date BETWEEN '2023-07-01' AND '2024-02-01'
INTERSECT
SELECT status, order_id
FROM shipments
WHERE cost <= 26
