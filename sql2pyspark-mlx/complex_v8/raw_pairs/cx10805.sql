WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE operator IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(days_since_moved_at) IGNORE NULLS OVER (PARTITION BY move_type, warehouse_id ORDER BY days_since_moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_moved_at,
        MAX(days_since_moved_at) OVER (PARTITION BY move_type) AS move_type_max_days_since_moved_at,
        ROUND(days_since_moved_at - AVG(days_since_moved_at) OVER (PARTITION BY move_type), 2) AS days_since_moved_at_vs_move_type_avg,
        MIN(days_since_moved_at) OVER w_move_type AS move_type_min_days_since_moved_at,
        LEAD(days_since_moved_at, 3) OVER (PARTITION BY move_type, warehouse_id ORDER BY days_since_moved_at, id) AS next3_days_since_moved_at,
        LAG(moved_at, 3) OVER (PARTITION BY move_type, sku_id ORDER BY days_since_moved_at, id) AS prev3_moved_at
    FROM filtered_moves
    WINDOW w_move_type AS (PARTITION BY move_type)
),
latest AS (
    -- Keep the latest row per sku_id, move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        days_since_moved_at,
        last_known_days_since_moved_at,
        move_type_max_days_since_moved_at,
        days_since_moved_at_vs_move_type_avg,
        move_type_min_days_since_moved_at,
        next3_days_since_moved_at,
        prev3_moved_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id, move_type ORDER BY move_type_max_days_since_moved_at DESC, id DESC) AS recency_rank
        FROM metrics
    ) ranked
    WHERE recency_rank = 1
),
latest_2 AS (
    -- Keep the latest row per warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        days_since_moved_at,
        last_known_days_since_moved_at,
        move_type_max_days_since_moved_at,
        days_since_moved_at_vs_move_type_avg,
        move_type_min_days_since_moved_at,
        next3_days_since_moved_at,
        prev3_moved_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY moved_at DESC, id DESC) AS row_num
        FROM latest
    ) ranked
    WHERE row_num = 1
),
filtered AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM latest_2 s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.unit_price > 119.5
              AND st.moved_at >= TIMESTAMP '2025-01-19 09:00:00'
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT warehouse_id, moved_at, move_type, days_since_moved_at_vs_move_type_avg
    FROM filtered
) src
PIVOT (
    MIN(days_since_moved_at_vs_move_type_avg)
    FOR move_type IN ('TRANSFER', 'ADJUST', 'INBOUND')
)
ORDER BY warehouse_id, moved_at
