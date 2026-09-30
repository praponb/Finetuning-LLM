SELECT brand, category, SUM(cost) AS total_cost
FROM products
GROUP BY brand, category
HAVING total_cost > 3200
ORDER BY brand
