SELECT status, 'order' AS source FROM orders UNION ALL SELECT status, 'shipment' AS source FROM shipments ORDER BY status DESC
