SELECT ss.id, ss.sale_date, ss.units, ss.revenue FROM store_sales ss WHERE ss.store_id IN (3, 4, 5) AND ss.product_id NOT IN (SELECT id FROM products WHERE brand <> 'Globex')
