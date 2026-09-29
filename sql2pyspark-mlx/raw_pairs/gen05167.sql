SELECT ss.id, ss.store_id, ss.units FROM store_sales ss WHERE ss.sale_date > '2024-05-01' AND ss.store_id IN (SELECT id FROM stores WHERE store_name IS NOT NULL)
