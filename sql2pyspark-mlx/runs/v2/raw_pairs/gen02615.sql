SELECT id, supplier_id, stock_qty, RANK() OVER (PARTITION BY supplier_id ORDER BY stock_qty DESC) AS stock_qty_rank FROM products;
