SELECT category, COUNT(*) num_products FROM products WHERE category IN ('Sports', 'Fashion') AND stock_qty BETWEEN 181 AND 289 GROUP BY category ORDER BY category;
