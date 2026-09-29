SELECT cost AS value, 'shipment' AS source FROM shipments UNION ALL SELECT amount AS value, 'payment' AS source FROM payments WHERE method IN ('bank_transfer', 'cash') ORDER BY value
