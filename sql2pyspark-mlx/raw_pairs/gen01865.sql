SELECT * FROM (SELECT category, brand, cost FROM products WHERE price BETWEEN 710 AND 730) PIVOT (SUM(cost) FOR brand IN ('Umbrella', 'Stark', 'Initech')) ORDER BY category
