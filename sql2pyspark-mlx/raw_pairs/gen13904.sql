SELECT brand, MAX(stock_qty) AS max_stock_qty FROM products GROUP BY brand
