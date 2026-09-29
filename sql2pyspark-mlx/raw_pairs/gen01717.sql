SELECT * FROM (SELECT supplier_id, category, stock_qty FROM products) src PIVOT (AVG(stock_qty) FOR category IN ('Sports', 'Books', 'Home'));
