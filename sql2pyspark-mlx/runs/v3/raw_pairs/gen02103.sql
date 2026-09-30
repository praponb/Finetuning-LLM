SELECT * FROM (SELECT category, brand, price FROM products) PIVOT (AVG(price) FOR brand IN ('Initech', 'Umbrella', 'Stark')) ORDER BY category;
