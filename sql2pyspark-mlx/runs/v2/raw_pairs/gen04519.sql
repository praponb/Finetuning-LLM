SELECT * FROM (SELECT brand, category, price FROM products WHERE cost >= 240) PIVOT (MAX(price) FOR category IN ('Home', 'Sports', 'Grocery')) ORDER BY brand
