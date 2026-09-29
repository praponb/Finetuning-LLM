SELECT id, brand, stock_qty, price, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty, id) rn, NTILE(4) OVER (ORDER BY price DESC, id) quartile FROM products
