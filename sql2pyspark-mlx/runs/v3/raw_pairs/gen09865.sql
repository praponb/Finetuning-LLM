SELECT
    orders.id,
    orders.status,
    COUNT(shipments.id) AS shipment_count,
    SUM(CASE WHEN shipments.carrier = 'Kerry' THEN 1 ELSE 0 END) AS kerry_count
FROM orders
JOIN shipments ON shipments.order_id = orders.id
GROUP BY orders.id, orders.status
ORDER BY orders.id DESC;
