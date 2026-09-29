SELECT ss.id, ss.sale_date, ss.store_id, ss.product_id FROM store_sales ss WHERE ss.store_id IN (1, 6) AND ss.store_id IN (SELECT id FROM stores WHERE city = 'New York') ORDER BY id DESC
