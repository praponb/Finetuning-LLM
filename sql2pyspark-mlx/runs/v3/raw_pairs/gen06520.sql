SELECT * FROM (SELECT brand, category, cost FROM products WHERE stock_qty BETWEEN 128 AND 285)
PIVOT (COUNT(cost) FOR category IN ('Home', 'Sports'));
