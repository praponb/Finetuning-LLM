SELECT id, category, stock_qty, ROW_NUMBER() OVER (PARTITION BY category ORDER BY stock_qty DESC, id) AS stock_qty_row FROM products;
