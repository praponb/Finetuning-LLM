SELECT
    id,
    product_name,
    category,
    brand,
    price,
    cost,
    stock_qty,
    launch_date,
    tags,
    supplier_id,
    ROUND(cost / SUM(cost) OVER (PARTITION BY category), 4) AS cost_share,
    LEAD(cost, 1) OVER (PARTITION BY category, supplier_id ORDER BY launch_date, id) AS next_cost,
    RANK() OVER (ORDER BY cost NULLS FIRST) AS cost_rank
FROM products;
