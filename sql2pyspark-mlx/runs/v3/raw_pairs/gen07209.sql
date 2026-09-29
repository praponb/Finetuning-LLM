SELECT status, 'payment' AS source FROM payments WHERE status IN ('REFUNDED', 'SUCCESS', 'FAILED') UNION SELECT status, 'shipment' AS source FROM shipments ORDER BY source DESC;
