SELECT
    id,
    category,
    launch_date,
    cost,
    supplier_id,
    cost - LAG(cost, 1) OVER (PARTITION BY category ORDER BY launch_date, id) AS cost_change,
    LAG(cost, 1) OVER (PARTITION BY supplier_id ORDER BY launch_date, id) AS prev_cost
FROM products
