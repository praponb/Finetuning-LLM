SELECT
    id,
    brand,
    launch_date,
    stock_qty,
    SUM(stock_qty) OVER (PARTITION BY brand ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_stock_qty
FROM products;
