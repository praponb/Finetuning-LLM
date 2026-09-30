SELECT category, ROUND((100 * SUM(CASE WHEN brand = 'Stark' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_stark FROM products WHERE cost >= 660 GROUP BY category ORDER BY category LIMIT 5;
