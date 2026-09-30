SELECT * FROM (SELECT category, brand, price FROM products) PIVOT (COUNT(price) FOR brand IN ('Initech', 'Globex')) ORDER BY category;
