SELECT category, row_count FROM (SELECT category, COUNT(*) AS row_count FROM products WHERE product_name LIKE 'Lap%' GROUP BY category) AS s WHERE row_count >= 6 ORDER BY row_count
