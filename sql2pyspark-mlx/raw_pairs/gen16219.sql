SELECT AVG(cost) AS avg_cost, ROUND((100 * SUM(CASE WHEN brand = 'Globex' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_globex FROM products WHERE product_name LIKE '%Des%' AND cost >= 440
