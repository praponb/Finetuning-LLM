WITH deduped AS (
    SELECT
        id,
        cost,
        stock_qty,
        tags
    FROM products
    QUALIFY ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY stock_qty, id) = 1
),
not_flagged AS (
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.store_id IN (3, 4, 5, 6)
        )
),
with_windows AS (
    SELECT
        id,
        cost,
        stock_qty,
        tags,
        AVG(cost) OVER (ORDER BY cost, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_cost,
        SUM(cost) OVER (ORDER BY cost, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_cost,
        MAX(cost) OVER (ORDER BY cost, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_cost,
        DENSE_RANK() OVER (ORDER BY cost DESC) AS cost_dense_rank
    FROM not_flagged
),
metrics AS (
    SELECT
        id,
        stock_qty,
        tags,
        last7_avg_cost,
        last7_sum_cost,
        cost_dense_rank,
        SUM(cost) OVER (ORDER BY last7_avg_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
        CUME_DIST() OVER (ORDER BY last7_max_cost) AS last7_max_cost_cume_dist,
        CUME_DIST() OVER (ORDER BY cost DESC NULLS FIRST) AS cost_cume_dist
    FROM with_windows
),
filtered AS (
    SELECT
        *,
        CASE WHEN stock_qty >= 322 THEN 'High' WHEN stock_qty >= 155 THEN 'Medium' ELSE 'Low' END AS stock_qty_band,
        CASE WHEN last7_max_cost_cume_dist >= 0.78 THEN 'High' WHEN last7_max_cost_cume_dist >= 0.22 THEN 'Medium' ELSE 'Low' END AS last7_max_cost_cume_dist_level
    FROM metrics
    WHERE last7_max_cost_cume_dist <= 0.3
)
SELECT
    id,
    stock_qty,
    tags,
    last7_avg_cost,
    cost_dense_rank,
    cumulative_cost,
    cost_cume_dist
FROM filtered
