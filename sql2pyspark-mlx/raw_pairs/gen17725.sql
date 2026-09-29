SELECT id, supplier_id, stock_qty, RANK() OVER (PARTITION BY supplier_id ORDER BY stock_qty) AS stock_qty_rank FROM products WHERE price >= 700;
