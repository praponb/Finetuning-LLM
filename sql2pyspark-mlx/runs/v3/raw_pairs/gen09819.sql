SELECT * FROM (SELECT brand, category, price FROM products) PIVOT (AVG(price) FOR category IN ('Electronics', 'Grocery')) ORDER BY brand;
