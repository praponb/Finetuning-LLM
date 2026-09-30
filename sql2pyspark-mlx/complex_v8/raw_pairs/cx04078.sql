WITH filtered_warehouses AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        ABS(capacity - 11506) AS capacity_distance
    FROM warehouses
    WHERE opened_on >= DATE '2024-11-25'
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM filtered_warehouses s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.sku_id IS NOT NULL
              AND st.moved_at < TIMESTAMP '2025-02-24 11:00:00'
        )
      AND s.capacity_distance >= 6000
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY capacity_distance, id) AS capacity_distance_rank,
        LAG(capacity_distance, 2, 0) OVER (ORDER BY opened_on, id) AS prev2_capacity_distance,
        ROUND(CUME_DIST() OVER w_all_by_opened_on, 3) AS opened_on_cume_dist,
        FIRST_VALUE(opened_on) IGNORE NULLS OVER (ORDER BY capacity_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_on,
        MAX(capacity) OVER (ORDER BY opened_on, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS capacity_max_3_prior,
        NTH_VALUE(capacity, 2) OVER (ORDER BY capacity, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_capacity
    FROM qualified
    WINDOW w_all_by_opened_on AS (ORDER BY opened_on, id)
),
stats AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', first_known_opened_on) AS first_known_opened_on_month,
        capacity_distance_rank,
        COUNT(*) AS n_rows,
        MIN(capacity_distance_rank) AS min_capacity_distance_rank
    FROM metrics
    GROUP BY 1, 2
    HAVING SUM(prev2_capacity_distance) > 5000
)
-- Final projection
SELECT
    first_known_opened_on_month,
    n_rows,
    CASE WHEN n_rows >= 8 THEN 'HIGH' WHEN n_rows >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS n_rows_tier
FROM stats
ORDER BY n_rows
