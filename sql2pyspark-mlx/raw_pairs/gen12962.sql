SELECT * FROM (SELECT category, brand, cost FROM products) src PIVOT (SUM(cost) AS total FOR brand IN ('Stark', 'Umbrella', 'Initech'))
