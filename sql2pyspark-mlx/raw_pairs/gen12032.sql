SELECT * FROM (SELECT brand, category, stock_qty FROM products) src PIVOT (AVG(stock_qty) AS amount, COUNT(1) AS n FOR category IN ('Sports', 'Home'))
