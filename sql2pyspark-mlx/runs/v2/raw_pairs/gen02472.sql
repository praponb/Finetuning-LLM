SELECT * FROM (SELECT brand, category, cost FROM products) PIVOT (SUM(cost) FOR category IN ('Books', 'Electronics')) ORDER BY brand;
