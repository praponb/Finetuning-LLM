SELECT
    id,
    brand,
    launch_date,
    stock_qty,
    SUM(stock_qty) OVER (PARTITION BY brand ORDER BY launch_date, id) AS running_stock_qty
FROM products
WHERE product_name IS NOT NULL
