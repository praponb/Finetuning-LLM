SELECT orders.store_id, orders.status, shipments.cost
FROM orders
INNER JOIN shipments ON orders.id = shipments.order_id
WHERE orders.shipping_fee < 22 AND shipments.status IN ('IN_TRANSIT', 'DELIVERED')
