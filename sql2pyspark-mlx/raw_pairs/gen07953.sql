SELECT
    id,
    brand,
    tags,
    launch_date,
    stock_qty * cost AS stock_qty_times_cost
FROM products
WHERE product_name LIKE 'La%'
ORDER BY id DESC
