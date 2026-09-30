SELECT * FROM (SELECT category, brand, cost FROM products) PIVOT (SUM(cost) FOR brand IN ('Initech', 'Stark', 'Umbrella', 'Globex')) ORDER BY category;
