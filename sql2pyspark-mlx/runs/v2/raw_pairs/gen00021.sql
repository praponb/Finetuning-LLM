SELECT brand, MIN(cost) AS min_cost, MAX(cost) - MIN(cost) AS cost_range
FROM products
WHERE product_name LIKE 'No%' AND product_name IS NULL AND ARRAY_CONTAINS(tags, 'sale')
GROUP BY brand
HAVING MIN(cost) >= 510;
