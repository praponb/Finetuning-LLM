SELECT o.id, o.status, COUNT(s.id) AS shipment_count FROM orders o LEFT JOIN shipments s ON s.order_id = o.id GROUP BY o.id, o.status ORDER BY shipment_count DESC, status;
