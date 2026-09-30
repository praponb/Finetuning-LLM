SELECT ss.id, ss.product_id FROM store_sales ss WHERE ss.product_id IN (2, 7, 9, 10) AND ss.store_id NOT IN (SELECT id FROM stores WHERE region <> 'North');
