SELECT status, 'payment' AS source FROM payments WHERE amount BETWEEN 650 AND 1040 UNION SELECT status, 'shipment' AS source FROM shipments
