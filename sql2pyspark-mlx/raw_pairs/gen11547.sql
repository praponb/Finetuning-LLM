WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(cost) OVER (PARTITION BY brand ORDER BY stock_qty ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_cost,
        MIN(cost) OVER (PARTITION BY brand ORDER BY stock_qty ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_cost,
        COUNT(cost) OVER (PARTITION BY brand ORDER BY stock_qty ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_cost,
        RANK() OVER (PARTITION BY YEAR(launch_date), brand ORDER BY stock_qty ASC) AS stock_qty_rank
    FROM products
)
SELECT
    id,
    product_name,
    category,
    brand,
    price,
    cost,
    stock_qty,
    launch_date,
    tags
FROM enriched
