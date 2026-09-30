SELECT shipments.cost, orders.order_date, orders.store_id
FROM shipments
FULL OUTER JOIN orders ON shipments.order_id = orders.id
