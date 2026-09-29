SELECT s.id, s.carrier
FROM shipments s
WHERE s.delivered_date BETWEEN '2023-07-13' AND '2025-06-02' AND s.order_id NOT IN (SELECT id FROM orders WHERE order_date < '2023-04-01');
