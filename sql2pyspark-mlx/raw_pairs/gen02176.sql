SELECT category, ROUND((100 * SUM(CASE WHEN category = 'Fashion' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_fashion, ROUND(AVG(stock_qty), 2) AS avg_stock_qty
FROM products
WHERE SIZE(tags) >= 2
GROUP BY category;
