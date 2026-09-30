SELECT
    id,
    launch_date,
    stock_qty,
    product_name
FROM products
WHERE launch_date BETWEEN '2023-12-01' AND '2024-01-01' OR stock_qty < 378
