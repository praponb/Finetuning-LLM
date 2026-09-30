SELECT
    id,
    brand,
    cost,
    category,
    stock_qty,
    DENSE_RANK() OVER (PARTITION BY brand ORDER BY cost) AS cost_dense_rank,
    RANK() OVER (PARTITION BY category ORDER BY stock_qty) AS stock_qty_rank
FROM products
ORDER BY stock_qty;
