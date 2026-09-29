WITH valid AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        HOUR(moved_at) AS moved_at_hour,
        DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label
    FROM stock_moves
    WHERE qty <= 415
      AND warehouse_id IS NOT NULL
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, COUNT(*) AS move_count, COUNT(*) AS move_count_2
    FROM stock_moves
    GROUP BY warehouse_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        COALESCE(st.move_count_2, 0) AS move_count_2
    FROM valid s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        moved_at_hour,
        moved_at_label,
        move_count,
        move_count_2,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label_2,
        unit_price + qty AS unit_price_plus_qty
    FROM with_counts
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        moved_at_hour,
        moved_at_label,
        move_count,
        move_count_2,
        unit_price_plus_qty,
        ROUND(CUME_DIST() OVER (ORDER BY unit_price_plus_qty DESC, id), 3) AS unit_price_plus_qty_cume_dist,
        NTH_VALUE(unit_price_plus_qty, 2) OVER (ORDER BY move_count ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price_plus_qty
    FROM enriched
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT sku_id, operator, unit_price_plus_qty_cume_dist
    FROM ranked
) src
PIVOT (
    MIN(unit_price_plus_qty_cume_dist) AS min_v, SUM(unit_price_plus_qty_cume_dist) AS sum_v
    FOR operator IN ('op_eve', 'op_chan', 'op_dew', 'op_bee')
)
