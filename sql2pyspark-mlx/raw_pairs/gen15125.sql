SELECT * FROM (SELECT brand, category, price FROM products) PIVOT (MAX(price) FOR category IN ('Books', 'Home', 'Electronics', 'Sports')) ORDER BY brand;
