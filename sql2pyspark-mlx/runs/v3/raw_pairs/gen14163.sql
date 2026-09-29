SELECT s.sqft, s.city, ss.revenue FROM stores s INNER JOIN store_sales ss ON ss.store_id = s.id WHERE s.region != 'Central';
