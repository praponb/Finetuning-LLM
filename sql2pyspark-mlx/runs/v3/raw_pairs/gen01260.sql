SELECT revenue, 'store_sale' AS source FROM store_sales WHERE store_id IN (1, 2) UNION SELECT cost AS revenue, 'product' AS source FROM products WHERE LENGTH(product_name) > 6 ORDER BY revenue
