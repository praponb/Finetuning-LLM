SELECT * FROM (SELECT brand, category, price FROM products WHERE price IS NOT NULL) PIVOT (SUM(price) FOR category IN ('Books', 'Grocery', 'Sports'));
