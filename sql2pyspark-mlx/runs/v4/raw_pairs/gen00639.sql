SELECT ss.id, ss.store_id, ss.revenue FROM store_sales ss WHERE ss.revenue > 3200 AND ss.store_id NOT IN (SELECT id FROM stores WHERE YEAR(opened_date) >= 2023);
