SELECT * FROM (SELECT brand, category, stock_qty FROM products) PIVOT (SUM(stock_qty) FOR category IN ('Sports', 'Grocery'));
