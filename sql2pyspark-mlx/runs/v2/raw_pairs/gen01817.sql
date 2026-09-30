SELECT shipments.shipped_date, orders.channel
FROM shipments
LEFT OUTER JOIN orders ON shipments.order_id = orders.id
