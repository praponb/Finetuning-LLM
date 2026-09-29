SELECT
    id,
    launch_date,
    brand,
    stock_qty,
    LAG(stock_qty, 2, 0) OVER (PARTITION BY YEAR(launch_date), brand ORDER BY launch_date, id) AS prev_stock_qty
FROM products
WHERE price < 1010;
