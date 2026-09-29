SELECT id, cost, status, order_id FROM shipments WHERE cost > (SELECT MAX(cost) FROM shipments WHERE status = 'IN_TRANSIT')
