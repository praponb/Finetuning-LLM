SELECT orders.status, COUNT(shipments.id) AS shipment_count, MAX(shipments.cost) AS max_cost
FROM orders
INNER JOIN shipments ON orders.id = shipments.order_id
WHERE orders.channel IN ('store', 'mobile', 'web')
GROUP BY orders.status
HAVING shipment_count >= 1
ORDER BY max_cost DESC;
