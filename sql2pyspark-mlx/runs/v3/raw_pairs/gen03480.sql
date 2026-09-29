SELECT delivered_date, 'shipment' source FROM shipments UNION ALL SELECT launch_date, 'product' source FROM products WHERE SIZE(tags) >= 2;
