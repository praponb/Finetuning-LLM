WITH scored AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        AVG(cost) OVER (PARTITION BY category, brand ORDER BY launch_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cost_avg_6,
        FIRST_VALUE(cost) IGNORE NULLS OVER (PARTITION BY brand, category ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost,
        SUM(cost) OVER (PARTITION BY category) AS category_sum_cost
    FROM products
)
SELECT
    id,
    product_name,
    category,
    tags,
    cost_avg_6,
    category_sum_cost
FROM scored
ORDER BY id DESC, category_sum_cost
