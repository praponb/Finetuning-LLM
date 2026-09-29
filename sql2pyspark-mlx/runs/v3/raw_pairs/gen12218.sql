SELECT ss.id, ss.units, ss.store_id, ss.sale_date FROM store_sales ss WHERE ss.product_id NOT IN (SELECT id FROM products WHERE product_name NOT LIKE '%Sma%') ORDER BY store_id DESC, id
