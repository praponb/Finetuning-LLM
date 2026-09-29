SELECT brand, MIN(cost) AS min_cost
FROM products
GROUP BY brand;
