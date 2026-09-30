SELECT launch_date AS event_date FROM products WHERE stock_qty >= 308 UNION SELECT last_restock AS event_date FROM inventory
