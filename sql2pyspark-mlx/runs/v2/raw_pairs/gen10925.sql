SELECT
    id,
    supplier_id,
    category,
    launch_date,
    cost,
    LAG(cost, 1, 0) OVER (PARTITION BY supplier_id, category ORDER BY launch_date, id) AS prev_cost
FROM products
WHERE stock_qty BETWEEN 180 AND 201
