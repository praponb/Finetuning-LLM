SELECT launch_date FROM products UNION ALL SELECT opened_date AS launch_date FROM stores WHERE store_name IS NOT NULL;
