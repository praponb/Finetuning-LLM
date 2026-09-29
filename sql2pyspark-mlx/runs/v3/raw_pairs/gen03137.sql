SELECT shipments.id, shipments.status, shipments.delivered_date, shipments.carrier FROM shipments WHERE shipments.order_id NOT IN (SELECT id FROM orders WHERE status != 'COMPLETED');
