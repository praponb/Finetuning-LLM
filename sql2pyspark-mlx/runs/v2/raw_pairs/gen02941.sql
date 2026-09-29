SELECT
    id,
    launch_date,
    stock_qty,
    SUM(stock_qty) OVER (ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_stock_qty
FROM products
ORDER BY id DESC;
