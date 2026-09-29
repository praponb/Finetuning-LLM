SELECT * FROM (SELECT category, brand, cost FROM products)
PIVOT (AVG(cost) FOR brand IN ('Stark', 'Acme', 'Globex', 'Initech'))
ORDER BY category;
