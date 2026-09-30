SELECT ss.id, ss.product_id, ss.revenue, ss.sale_date FROM store_sales ss WHERE ss.product_id IN (SELECT id FROM products WHERE brand <> 'Initech') ORDER BY revenue DESC
