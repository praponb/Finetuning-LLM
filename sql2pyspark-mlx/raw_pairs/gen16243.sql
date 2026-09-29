SELECT s.sqft, ss.product_id, ss.sale_date, ss.units FROM stores s INNER JOIN store_sales ss ON s.id = ss.store_id ORDER BY units;
