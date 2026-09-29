SELECT shipments.delivered_date, shipments.order_id, orders.status
FROM shipments
JOIN orders ON shipments.order_id = orders.id
WHERE shipments.order_id IN (37, 39) AND orders.shipping_fee < 31
