SELECT id, brand, ROUND(cost * 1.15, 2) AS cost_scaled
FROM products
ORDER BY brand ASC, id ASC
LIMIT 10
