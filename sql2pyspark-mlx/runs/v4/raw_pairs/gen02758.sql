SELECT status, 'payment' source FROM payments UNION ALL SELECT status, 'shipment' source FROM shipments
