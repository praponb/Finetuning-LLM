SELECT id, cost, order_id, carrier FROM shipments WHERE cost > (SELECT AVG(cost) FROM shipments)
