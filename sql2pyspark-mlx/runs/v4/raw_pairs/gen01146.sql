SELECT s.store_name, s.sqft, s.region, ss.units FROM stores s LEFT JOIN store_sales ss ON s.id = ss.store_id WHERE YEAR(s.opened_date) >= 2023 AND ss.product_id IN (3, 4, 9, 10);
