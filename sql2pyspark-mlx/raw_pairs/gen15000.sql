SELECT launch_date, 'product' AS record_type FROM products WHERE launch_date IS NOT NULL UNION SELECT delivered_date, 'shipment' AS record_type FROM shipments WHERE delivered_date IS NOT NULL
