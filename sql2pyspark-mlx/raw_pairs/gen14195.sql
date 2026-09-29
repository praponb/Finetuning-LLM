SELECT * FROM (SELECT category, brand, cost FROM products) PIVOT (COUNT(cost) FOR brand IN ('Umbrella', 'Initech')) ORDER BY category
