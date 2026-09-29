SELECT shipments.order_id, shipments.carrier, orders.store_id FROM shipments INNER JOIN orders ON shipments.order_id = orders.id WHERE orders.channel NOT IN ('web', 'mobile') OR orders.amount <= 1450
