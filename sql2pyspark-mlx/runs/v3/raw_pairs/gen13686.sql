SELECT status, 'order' source FROM orders UNION SELECT status, 'shipment' source FROM shipments ORDER BY status;
