SELECT cost, 'shipment' AS source FROM shipments WHERE order_id IN (19, 26, 30) UNION ALL SELECT amount AS cost, 'order' AS source FROM orders
