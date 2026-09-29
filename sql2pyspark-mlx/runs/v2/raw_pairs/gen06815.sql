SELECT
    id,
    launch_date,
    stock_qty,
    category,
    brand,
    SUM(stock_qty) OVER (ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_stock_qty,
    FIRST_VALUE(brand) OVER (PARTITION BY category ORDER BY launch_date, id) AS first_brand
FROM products
ORDER BY running_stock_qty;
