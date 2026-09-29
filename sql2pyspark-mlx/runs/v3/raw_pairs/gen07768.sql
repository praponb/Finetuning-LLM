SELECT id, stock_qty, PERCENT_RANK() OVER (ORDER BY stock_qty) stock_qty_pct_rank
FROM products
WHERE price BETWEEN 1090 AND 1180;
