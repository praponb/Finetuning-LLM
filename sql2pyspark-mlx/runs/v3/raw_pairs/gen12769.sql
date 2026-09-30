SELECT id, stock_qty, product_name, cost FROM products WHERE stock_qty >= (SELECT MAX(stock_qty) FROM products WHERE supplier_id IN (19, 24, 25, 28)) ORDER BY cost DESC, id DESC
