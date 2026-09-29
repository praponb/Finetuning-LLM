SELECT status, 'payment' AS source FROM payments UNION ALL SELECT status, 'shipment' AS source FROM shipments WHERE cost > 41
