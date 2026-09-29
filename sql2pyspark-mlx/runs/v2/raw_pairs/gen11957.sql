SELECT status, 'order' AS source FROM orders WHERE channel != 'web' UNION ALL SELECT status, 'shipment' AS source FROM shipments WHERE cost < 71
