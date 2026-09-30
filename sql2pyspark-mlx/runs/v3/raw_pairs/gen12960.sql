SELECT o.channel, COUNT(s.id) AS shipment_count FROM orders o LEFT JOIN shipments s ON s.order_id = o.id WHERE s.status = 'RETURNED' GROUP BY o.channel ORDER BY channel;
