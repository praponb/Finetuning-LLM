WITH base_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE sku_id <= 19
      AND operator IS NOT NULL
),
without_matches AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM base_moves s
    WHERE EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND UPPER(TRIM(w.wh_code)) LIKE 'K%'
              AND w.capacity BETWEEN 2614 AND 11506
        )
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(TRIM(operator)) AS operator_clean,
        INITCAP(TRIM(operator)) AS operator_clean_2
    FROM without_matches
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        operator_clean,
        COUNT(*) OVER (PARTITION BY warehouse_id) AS rows_in_warehouse_id,
        ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY moved_at DESC, id) AS moved_at_seq,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY warehouse_id)) AS distinct_sku_id_in_warehouse_id,
        ROUND(CUME_DIST() OVER w_sku_id_by_moved_at_ties, 3) AS moved_at_cume_dist
    FROM labeled
    WINDOW w_sku_id_by_moved_at_ties AS (PARTITION BY sku_id ORDER BY moved_at DESC)
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, MAX(unit_price) AS max_move_unit_price
    FROM stock_moves
    GROUP BY warehouse_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.max_move_unit_price
    FROM with_windows s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    operator_clean,
    rows_in_warehouse_id,
    moved_at_seq,
    distinct_sku_id_in_warehouse_id,
    moved_at_cume_dist
FROM with_counts
WHERE distinct_sku_id_in_warehouse_id <= 6
ORDER BY moved_at_cume_dist
