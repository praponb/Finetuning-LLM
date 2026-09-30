WITH cleaned_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at,
        CONCAT(operator, '#', CAST(sku_id AS STRING)) AS operator_ref,
        moved_at + INTERVAL 7 DAYS AS moved_at_plus_7d
    FROM stock_moves
    WHERE TO_DATE(moved_at) < DATE '2025-02-24'
      AND unit_price BETWEEN 37.6 AND 313.8
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        days_since_moved_at,
        operator_ref,
        LAST_VALUE(moved_at_plus_7d) OVER (PARTITION BY MONTH(moved_at) ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at_plus_7d,
        FIRST_VALUE(moved_at) OVER (PARTITION BY MONTH(moved_at) ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at
    FROM cleaned_moves
),
deduped AS (
    -- Keep the latest row per move_type
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        days_since_moved_at,
        operator_ref,
        last_moved_at_plus_7d,
        first_moved_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY qty DESC, id DESC) AS row_num
        FROM with_windows
    ) ranked
    WHERE row_num = 1
)
SELECT
    COALESCE(move_type, 'ALL') AS move_type,
    warehouse_id,
    COUNT(*) AS n_rows
FROM deduped
GROUP BY GROUPING SETS ((move_type, warehouse_id), (move_type), (warehouse_id))
ORDER BY move_type NULLS LAST, warehouse_id NULLS LAST
