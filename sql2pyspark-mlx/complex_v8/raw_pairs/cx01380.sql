WITH recent AS (
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
        DATE_TRUNC('HOUR', moved_at) AS moved_at_hour,
        YEAR(moved_at) AS moved_at_year,
        SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix
    FROM stock_moves
    WHERE (unit_price <= 361.9 OR warehouse_id >= 3)
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, MAX(moved_at) AS last_moved_at, MAX(qty) AS max_move_qty
    FROM stock_moves
    GROUP BY warehouse_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.last_moved_at,
        st.max_move_qty
    FROM recent s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        moved_at_year,
        operator_prefix,
        last_moved_at,
        max_move_qty,
        CASE WHEN move_type IN ('ADJUST', 'INBOUND') THEN 'group_a' WHEN move_type = 'OUTBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group,
        DATEDIFF(DATE '2025-06-30', last_moved_at) AS days_since_last_moved_at
    FROM with_counts
    WHERE qty < 422
)
SELECT
    sku_id,
    moved_at_year,
    GROUPING_ID() AS gid,
    COUNT(*) FILTER (WHERE move_type = 'OUTBOUND') AS n_move_type_outbound,
    SUM(max_move_qty) AS total_max_move_qty
FROM annotated
GROUP BY GROUPING SETS ((sku_id, moved_at_year), (sku_id), (moved_at_year), ())
