SELECT orders.channel, COUNT(shipments.id) AS shipment_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(shipments.carrier))) AS carrier_values
FROM orders
JOIN shipments ON orders.id = shipments.order_id
WHERE shipments.shipped_date > '2025-08-01'
GROUP BY orders.channel
ORDER BY carrier_values, channel
LIMIT 3
