SELECT o.store_id, COUNT(CASE WHEN s.carrier = 'UPS' THEN 1 END) AS ups_count, SUM(CASE WHEN s.carrier = 'Flash' THEN 1 ELSE 0 END) AS flash_count
FROM shipments s
INNER JOIN orders o ON o.id = s.order_id
GROUP BY o.store_id;
