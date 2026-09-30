SELECT id, category, stock_qty, DENSE_RANK() OVER (PARTITION BY category ORDER BY stock_qty) AS stock_qty_dense_rank FROM products WHERE ARRAY_CONTAINS(tags, 'bestseller')
