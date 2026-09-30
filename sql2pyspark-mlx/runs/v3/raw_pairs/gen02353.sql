SELECT brand, MAX(price) - MIN(price) AS price_range, AVG(cost) AS avg_cost FROM products GROUP BY brand HAVING COUNT(CASE WHEN brand = 'Initech' THEN 1 END) < 4;
