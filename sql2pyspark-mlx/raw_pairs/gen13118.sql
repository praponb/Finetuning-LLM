SELECT s.id, s.cost
FROM shipments s
WHERE s.shipped_date BETWEEN '2023-04-02' AND '2023-07-01' AND s.order_id NOT IN (SELECT id FROM orders WHERE order_date > '2025-01-27');
