SELECT id, stock_qty FROM products WHERE stock_qty <= (SELECT MIN(stock_qty) FROM products) ORDER BY 1 DESC, stock_qty DESC LIMIT 20
