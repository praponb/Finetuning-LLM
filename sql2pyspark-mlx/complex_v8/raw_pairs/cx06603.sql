WITH scoped_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        moved_at + INTERVAL 30 MINUTES AS moved_at_plus_30m,
        ABS(unit_price - 236.5) AS unit_price_distance,
        CONCAT(move_type, '#', CAST(warehouse_id AS STRING)) AS move_type_ref
    FROM stock_moves
    WHERE qty BETWEEN 85 AND 271
),
totals AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', moved_at_plus_30m) AS moved_at_plus_30m_month,
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        sku_id,
        ROUND(SUM(unit_price_distance), 2) AS total_unit_price_distance,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
        SUM(qty) AS total_qty,
        COUNT(DISTINCT operator) AS distinct_operators,
        COUNT(DISTINCT id) AS distinct_ids
    FROM scoped_moves
    GROUP BY 1, 2, 3
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM totals s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.moved_at < TIMESTAMP '2025-02-24 13:00:00'
        )
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY distinct_warehouse_ids, moved_at_plus_30m_month, moved_at_month, sku_id) AS distinct_warehouse_ids_dense_rank,
        LAST_VALUE(moved_at_plus_30m_month) OVER (PARTITION BY sku_id, DATE_TRUNC('MONTH', moved_at_plus_30m_month) ORDER BY total_qty ASC, moved_at_plus_30m_month, moved_at_month, sku_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at_plus_30m_month,
        MIN(total_unit_price_distance) OVER (PARTITION BY distinct_warehouse_ids, sku_id) AS distinct_warehouse_ids_sku_id_min_total_unit_pri,
        FIRST_VALUE(total_qty) OVER (ORDER BY distinct_warehouse_ids, moved_at_plus_30m_month, moved_at_month, sku_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_total_qty,
        LAST_VALUE(total_unit_price_distance) OVER (ORDER BY distinct_warehouse_ids, moved_at_plus_30m_month, moved_at_month, sku_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_total_unit_price_distance
    FROM with_activity
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(moved_at_plus_30m_month) - UNIX_TIMESTAMP(moved_at_month)) / 60, 1) AS minutes_moved_at_month_to_moved_at_plus_30m_mont,
        first_total_qty - total_qty AS first_total_qty_minus_total_qty
    FROM metrics
    WHERE total_qty < 1950
),
with_details_sku AS (
    -- Join skus
    SELECT
        s.moved_at_plus_30m_month,
        s.moved_at_month,
        s.sku_id,
        s.total_qty,
        s.distinct_operators,
        s.distinct_ids,
        s.last_moved_at_plus_30m_month,
        s.distinct_warehouse_ids_sku_id_min_total_unit_pri,
        s.last_total_unit_price_distance,
        s.minutes_moved_at_month_to_moved_at_plus_30m_mont,
        s.first_total_qty_minus_total_qty,
        sk.sku_code,
        sk.unit_cost AS sku_unit_cost,
        sk.weight_kg AS sku_weight_kg
    FROM scored s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
line_level AS (
    -- One row per move
    SELECT
        s.moved_at_plus_30m_month,
        s.moved_at_month,
        s.sku_id,
        s.total_qty,
        s.distinct_ids,
        s.last_moved_at_plus_30m_month,
        s.distinct_warehouse_ids_sku_id_min_total_unit_pri,
        s.last_total_unit_price_distance,
        s.minutes_moved_at_month_to_moved_at_plus_30m_mont,
        s.sku_code,
        s.sku_weight_kg,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.move_type
    FROM with_details_sku s
    JOIN stock_moves st ON st.sku_id = s.sku_id
)
-- Final projection
SELECT
    moved_at_plus_30m_month,
    moved_at_month,
    total_qty,
    distinct_ids,
    last_moved_at_plus_30m_month,
    distinct_warehouse_ids_sku_id_min_total_unit_pri,
    minutes_moved_at_month_to_moved_at_plus_30m_mont,
    sku_code,
    sku_weight_kg,
    move_id,
    move_moved_at
FROM line_level
ORDER BY sku_weight_kg, distinct_ids DESC
