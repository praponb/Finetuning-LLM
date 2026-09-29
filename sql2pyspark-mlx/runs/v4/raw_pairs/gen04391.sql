SELECT * FROM (SELECT brand, category, stock_qty FROM products) PIVOT (MAX(stock_qty) FOR category IN ('Home', 'Books', 'Fashion', 'Grocery'));
