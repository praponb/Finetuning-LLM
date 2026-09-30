SELECT stores.id, stores.city FROM stores WHERE stores.sqft != 7228 AND EXISTS (SELECT 1 FROM store_sales WHERE store_sales.store_id = stores.id AND store_sales.revenue >= 3400);
