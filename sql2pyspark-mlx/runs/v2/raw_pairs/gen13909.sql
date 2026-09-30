SELECT
    id,
    category,
    launch_date,
    stock_qty,
    LEAD(stock_qty, 1) OVER (PARTITION BY category ORDER BY launch_date, id) AS next_stock_qty
FROM products
WHERE product_name LIKE '%Water%'
ORDER BY launch_date
