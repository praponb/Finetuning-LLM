SELECT id, stock_qty, brand, category FROM products WHERE stock_qty = (SELECT MIN(stock_qty) FROM products);
