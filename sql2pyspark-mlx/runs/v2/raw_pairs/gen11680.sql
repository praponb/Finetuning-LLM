SELECT
    id,
    launch_date,
    stock_qty,
    category,
    COALESCE(cost, 0) cost_filled
FROM products
