SELECT * FROM (SELECT category, brand, cost FROM products)
PIVOT (SUM(cost) FOR brand IN ('Umbrella', 'Stark', 'Initech'))
ORDER BY category;
