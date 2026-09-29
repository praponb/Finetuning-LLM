SELECT id, stock_qty, ARRAY_JOIN(tags, '|') AS tags_text FROM products
