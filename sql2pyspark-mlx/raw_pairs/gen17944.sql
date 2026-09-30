SELECT orders.status, orders.channel, SUM(CASE WHEN shipments.carrier = 'Flash' THEN shipments.cost ELSE 0 END) AS flash_cost
FROM shipments
LEFT JOIN orders ON orders.id = shipments.order_id
WHERE shipments.status = 'RETURNED'
GROUP BY orders.status, orders.channel
ORDER BY orders.channel
