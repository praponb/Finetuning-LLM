SELECT 'product' AS origin, cost AS value FROM products WHERE cost IS NOT NULL UNION ALL SELECT 'store_sale' AS origin, revenue AS value FROM store_sales WHERE revenue IS NOT NULL ORDER BY origin;
