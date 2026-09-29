SELECT status, 'order' AS source FROM orders WHERE shipping_fee >= 12 UNION SELECT status, 'shipment' AS source FROM shipments
