WITH scoped_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        moved_at + INTERVAL 30 MINUTES AS moved_at_plus_30m,
        SUBSTRING(TRIM(operator), 1, 4) AS operator_prefix,
        DAYOFWEEK(moved_at) AS moved_at_dayofweek
    FROM stock_moves
    WHERE TO_DATE(moved_at) < DATE '2025-02-24'
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS min_move_qty,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count
    FROM scoped_moves s
),
move_stats_2 AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        COUNT(*) FILTER (WHERE move_type IN ('ADJUST', 'TRANSFER', 'OUTBOUND')) AS move_move_type_count,
        SUM(unit_price) AS total_move_unit_price
    FROM stock_moves
    GROUP BY sku_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_move_type_count, 0) AS move_move_type_count,
        st.total_move_unit_price
    FROM with_stats s
    LEFT JOIN move_stats_2 st ON st.sku_id = s.sku_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('WEEK', moved_at) AS moved_at_week,
        CASE WHEN operator IN ('op_chan', 'op_bee') THEN 'group_a' WHEN operator = 'op_ann' THEN 'group_b' ELSE 'other' END AS operator_group,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(moved_at_plus_30m)) / 60, 1) AS minutes_moved_at_plus_30m_to_moved_at
    FROM profiled
    WHERE moved_at_dayofweek < 6
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        moved_at_dayofweek,
        MIN(min_move_qty) AS min_min_move_qty,
        SUM(moved_at_dayofweek) AS total_moved_at_dayofweek,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(move_move_type_count) AS total_move_move_type_count,
        MIN(move_count) AS min_move_count,
        SUM(move_count) AS total_move_count
    FROM enriched
    GROUP BY moved_at_dayofweek
    HAVING AVG(move_count) > 2
),
enriched_2 AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(total_move_count - 24) AS total_move_count_distance,
        total_moved_at_dayofweek - min_min_move_qty AS total_moved_at_dayofweek_minus_min_min_move_qty
    FROM grouped
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        GREATEST(min_min_move_qty, total_move_count) AS greatest_min_min_move_qty_total_move_count,
        ABS(moved_at_dayofweek - 3) AS moved_at_dayofweek_distance
    FROM enriched_2
)
SELECT
    moved_at_dayofweek,
    min_min_move_qty,
    distinct_ids,
    total_move_move_type_count,
    min_move_count,
    total_move_count,
    total_move_count_distance,
    total_moved_at_dayofweek_minus_min_min_move_qty,
    moved_at_dayofweek_distance,
    CASE WHEN distinct_ids >= 8 THEN 'A' WHEN distinct_ids >= 4 THEN 'B' ELSE 'C' END AS distinct_ids_tier
FROM labeled
ORDER BY moved_at_dayofweek_distance DESC, total_move_move_type_count DESC
