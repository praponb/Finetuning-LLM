SELECT o.id, o.channel, COUNT(s.id) AS shipment_count
FROM orders o
JOIN shipments s ON s.order_id = o.id
GROUP BY o.id, o.channel
ORDER BY id DESC;
