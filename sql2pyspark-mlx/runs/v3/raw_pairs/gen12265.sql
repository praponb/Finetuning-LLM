SELECT last_restock AS event_date, 'inventory' AS source FROM inventory WHERE quantity >= 150 UNION ALL SELECT launch_date AS event_date, 'product' AS source FROM products;
