SELECT
    id,
    category,
    cost,
    brand,
    launch_date,
    stock_qty,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost, id) AS row_num,
    SUM(stock_qty) OVER (PARTITION BY brand ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_stock_qty
FROM products
