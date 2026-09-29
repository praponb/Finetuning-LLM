SELECT ss.id, ss.units, ss.sale_date FROM store_sales ss WHERE ss.store_id = 2 AND ss.store_id IN (SELECT id FROM stores WHERE sqft BETWEEN 10603 AND 15956) ORDER BY 2 DESC, id LIMIT 5
