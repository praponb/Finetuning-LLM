SELECT delivered_date FROM shipments WHERE delivered_date IS NOT NULL UNION SELECT launch_date FROM products WHERE launch_date IS NOT NULL;
