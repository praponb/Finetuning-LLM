SELECT s.store_name, s.region, ss.revenue, ss.units FROM stores s LEFT JOIN store_sales ss ON s.id = ss.store_id
