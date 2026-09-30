SELECT s.id, s.shipped_date
FROM shipments s
WHERE MONTH(s.delivered_date) IN (3, 9, 12) AND s.order_id NOT IN (SELECT id FROM orders WHERE order_date <= '2024-12-03');
