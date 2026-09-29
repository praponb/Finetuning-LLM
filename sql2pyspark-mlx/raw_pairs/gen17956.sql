SELECT brand, COUNT(*) AS row_count, ROUND((100 * SUM(CASE WHEN category = 'Sports' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_sports
FROM products
GROUP BY brand
