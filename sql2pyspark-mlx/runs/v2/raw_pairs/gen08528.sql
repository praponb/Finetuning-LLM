SELECT category, COUNT(DISTINCT stock_qty) AS unique_brands, ROUND((100 * SUM(CASE WHEN category = 'Electronics' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_electronics
FROM products
GROUP BY category
ORDER BY category ASC;
