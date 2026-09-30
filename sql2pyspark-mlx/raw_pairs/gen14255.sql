SELECT ss.id, ss.product_id, s.city, s.region FROM store_sales ss INNER JOIN stores s ON ss.store_id = s.id WHERE s.sqft < 8453;
