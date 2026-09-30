SELECT * FROM (SELECT category, brand, cost FROM products WHERE category = 'Sports') PIVOT (SUM(cost) FOR brand IN ('Globex', 'Acme')) ORDER BY category;
