SELECT shipments.id, shipments.status, shipments.cost FROM shipments WHERE shipments.order_id IN (SELECT id FROM orders WHERE status = 'RETURNED')
