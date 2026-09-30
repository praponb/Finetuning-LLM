SELECT ss.id, ss.product_id, ss.units, ss.sale_date FROM store_sales ss WHERE ss.store_id NOT IN (SELECT id FROM stores WHERE region IS NULL) ORDER BY product_id DESC, id LIMIT 20
