SELECT category, brand, ROUND((100 * SUM(CASE WHEN category = 'Grocery' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_grocery
FROM products
WHERE NOT (cost BETWEEN 240 AND 580 AND LENGTH(product_name) >= 10)
GROUP BY 1, 2
