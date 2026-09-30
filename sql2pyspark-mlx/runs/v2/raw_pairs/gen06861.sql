SELECT
    id,
    category,
    launch_date,
    stock_qty,
    ROUND(AVG(stock_qty) OVER (PARTITION BY category ORDER BY launch_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS stock_qty_moving_avg_3
FROM products;
