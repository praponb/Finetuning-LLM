SELECT status, 'order' AS source FROM orders UNION SELECT status, 'shipment' AS source FROM shipments WHERE status != 'RETURNED' ORDER BY status;
