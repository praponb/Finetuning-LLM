WITH recent_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at,
        CASE WHEN unit_price >= 307.8 THEN 'large' WHEN unit_price >= 160.8 THEN 'medium' ELSE 'small' END AS unit_price_tier
    FROM stock_moves
    WHERE qty > 189
      AND warehouse_id >= 3
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.operator_slug,
        s.unit_price_tier,
        st.id AS move_id,
        st.operator AS move_operator,
        st.qty AS move_qty,
        st.moved_at AS move_moved_at
    FROM recent_moves s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(move_moved_at, 2) OVER w_move_id_by_move_moved_at AS next2_move_moved_at,
        ROUND(AVG(move_qty) OVER (PARTITION BY sku_id), 2) AS sku_id_avg_move_qty,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_id ORDER BY move_qty ASC, id, move_id), 3) AS move_qty_cume_dist
    FROM with_children_moves
    WINDOW w_move_id_by_move_moved_at AS (PARTITION BY move_id ORDER BY move_moved_at, id, move_id)
),
detail_rows_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.operator_slug,
        s.unit_price_tier,
        s.move_id,
        s.move_operator,
        s.move_qty,
        s.move_moved_at,
        s.next2_move_moved_at,
        st.id AS move_id_2,
        st.moved_at AS move_moved_at_2,
        st.move_type,
        st.qty AS move_qty_2
    FROM ranked s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        operator_slug,
        unit_price_tier,
        move_id,
        move_operator,
        move_qty,
        move_moved_at,
        move_id_2,
        move_moved_at_2,
        LEAST(move_qty_2, move_qty) AS least_move_qty_2_move_qty
    FROM detail_rows_moves
)
SELECT id, move_id, metric, metric_value
FROM (
    SELECT id, move_id, least_move_qty_2_move_qty, move_qty
    FROM annotated
) src
UNPIVOT (
    metric_value FOR metric IN (least_move_qty_2_move_qty, move_qty)
) unp
ORDER BY id, move_id, metric
