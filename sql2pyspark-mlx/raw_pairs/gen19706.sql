SELECT orders.id, COUNT(shipments.id) AS shipment_count
FROM orders
LEFT JOIN shipments ON shipments.order_id = orders.id
WHERE orders.amount IS NULL
GROUP BY orders.id;
