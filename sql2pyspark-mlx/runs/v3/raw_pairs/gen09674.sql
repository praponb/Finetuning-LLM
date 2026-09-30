SELECT revenue, 'store_sale' AS source FROM store_sales UNION ALL SELECT price AS revenue, 'product' AS source FROM products WHERE price > 970 ORDER BY revenue
