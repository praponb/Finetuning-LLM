SELECT s.store_name, s.opened_date, s.region, ss.units, ss.store_id, ss.sale_date FROM stores s LEFT JOIN store_sales ss ON s.id = ss.store_id ORDER BY region;
