SELECT orders.id, COUNT(shipments.id) AS shipment_count, COUNT(CASE WHEN shipments.carrier = 'DHL' THEN 1 END) AS dhl_count
FROM orders
LEFT OUTER JOIN shipments ON orders.id = shipments.order_id
GROUP BY orders.id
HAVING COUNT(CASE WHEN shipments.carrier = 'DHL' THEN 1 END) >= 3
ORDER BY dhl_count DESC, shipment_count DESC;
