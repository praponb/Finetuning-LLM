SELECT ROUND(AVG(cost), 2) AS avg_cost
FROM products
WHERE supplier_id IN (4, 28, 32, 35) AND brand = 'Initech' AND price BETWEEN 380 AND 990
ORDER BY avg_cost ASC;
