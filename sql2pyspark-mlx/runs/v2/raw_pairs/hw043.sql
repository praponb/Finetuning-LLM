SELECT id, brand, stock_qty, DENSE_RANK() OVER (PARTITION BY brand ORDER BY stock_qty DESC) AS stock_rank FROM products;
