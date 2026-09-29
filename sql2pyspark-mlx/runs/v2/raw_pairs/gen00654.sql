SELECT s.city, s.region, s.sqft, ss.revenue FROM stores s LEFT JOIN store_sales ss ON s.id = ss.store_id
