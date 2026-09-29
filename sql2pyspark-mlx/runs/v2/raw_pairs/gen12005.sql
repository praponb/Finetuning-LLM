SELECT o.id, o.channel, COUNT(s.id) AS shipment_count
FROM orders o
LEFT JOIN shipments s ON o.id = s.order_id
GROUP BY o.id, o.channel;
