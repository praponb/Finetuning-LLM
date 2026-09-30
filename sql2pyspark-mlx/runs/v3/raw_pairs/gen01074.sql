SELECT * FROM (SELECT category, brand, price FROM products WHERE brand NOT IN ('Acme', 'Umbrella')) PIVOT (SUM(price) FOR brand IN ('Umbrella', 'Acme', 'Stark')) ORDER BY category;
