SELECT id, stock_qty FROM products WHERE stock_qty < (SELECT MIN(stock_qty) FROM products) ORDER BY 1 DESC
