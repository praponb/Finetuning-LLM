SELECT ss.id, ss.product_id, ss.revenue FROM store_sales ss WHERE ss.revenue >= 2200 AND ss.product_id IN (SELECT id FROM products WHERE supplier_id IN (22, 34))
