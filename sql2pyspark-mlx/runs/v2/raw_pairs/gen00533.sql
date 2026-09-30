SELECT o.status, COUNT(s.id) shipment_count, SUM(CASE WHEN s.carrier = 'Flash' THEN 1 ELSE 0 END) flash_count
FROM orders o
JOIN shipments s ON s.order_id = o.id
GROUP BY o.status;
