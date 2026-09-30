SELECT ss.id, ss.revenue FROM store_sales ss WHERE ss.units = 23 AND ss.store_id IN (SELECT id FROM stores WHERE store_name NOT LIKE '%Old%');
