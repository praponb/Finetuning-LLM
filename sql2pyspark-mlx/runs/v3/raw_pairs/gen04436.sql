SELECT launch_date, 'product' AS source FROM products UNION ALL SELECT delivered_date, 'shipment' AS source FROM shipments WHERE order_id IS NOT NULL
