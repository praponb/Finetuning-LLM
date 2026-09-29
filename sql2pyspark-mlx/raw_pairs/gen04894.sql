SELECT * FROM (SELECT brand, category, price FROM products) PIVOT (SUM(price) FOR category IN ('Books', 'Home', 'Sports'));
