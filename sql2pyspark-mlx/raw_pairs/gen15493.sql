SELECT status, 'payment' origin FROM payments WHERE order_id IS NULL UNION ALL SELECT status, 'shipment' origin FROM shipments ORDER BY origin
