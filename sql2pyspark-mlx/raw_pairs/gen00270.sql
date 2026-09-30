SELECT s.id, s.cost
FROM shipments s
WHERE s.cost <= 40 AND s.order_id NOT IN (SELECT id FROM orders WHERE order_date BETWEEN '2024-10-01' AND '2025-02-09')
ORDER BY id DESC
