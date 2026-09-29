SELECT ss.id, ss.sale_date FROM store_sales ss WHERE ss.store_id IN (SELECT id FROM stores WHERE region IS NOT NULL);
