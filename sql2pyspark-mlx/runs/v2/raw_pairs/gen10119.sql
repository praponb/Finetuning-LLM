SELECT id, price, brand, EXPLODE(tags) AS label FROM products WHERE stock_qty = 240
