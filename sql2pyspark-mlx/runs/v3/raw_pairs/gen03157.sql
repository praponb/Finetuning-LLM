SELECT * FROM (SELECT category, brand, cost FROM products)
PIVOT (SUM(cost) FOR brand IN ('Acme', 'Umbrella', 'Initech'))
ORDER BY category;
