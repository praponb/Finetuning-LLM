WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(cost) OVER (PARTITION BY brand ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_cost,
        AVG(cost) OVER (PARTITION BY brand ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_cost,
        MAX(cost) OVER (PARTITION BY brand ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_cost,
        MAX(cost) OVER (PARTITION BY category) category_max_cost
    FROM products
    WHERE ARRAY_CONTAINS(tags, 'new')
)
SELECT
    id,
    product_name,
    category,
    brand,
    price,
    tags,
    running_count_cost,
    running_avg_cost
FROM enriched
ORDER BY id DESC
