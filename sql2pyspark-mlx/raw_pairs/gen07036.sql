SELECT
    id,
    launch_date,
    stock_qty,
    RANK() OVER (PARTITION BY YEAR(launch_date) ORDER BY stock_qty DESC) AS rnk
FROM products
WHERE product_name IS NOT NULL
