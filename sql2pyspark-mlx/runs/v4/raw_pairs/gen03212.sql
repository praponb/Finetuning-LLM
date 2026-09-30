SELECT
    id,
    category,
    launch_date,
    stock_qty,
    LAG(stock_qty, 1) OVER (PARTITION BY category ORDER BY launch_date, id) AS prev_stock_qty
FROM products
ORDER BY stock_qty, launch_date;
