SELECT s.store_name, ss.product_id, ss.units
FROM stores s
LEFT JOIN store_sales ss ON ss.store_id = s.id;
