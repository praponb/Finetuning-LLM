WITH valid AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        ABS(unit_price - 161.4) AS unit_price_distance,
        FLOOR(qty / 100) * 100 AS qty_bucket,
        GREATEST(qty, unit_price) AS greatest_qty_unit_price
    FROM stock_moves
    WHERE operator IN ('op_eve', 'op_ann', 'op_dew')
      AND warehouse_id >= 5
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.unit_price_distance,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.move_type AS move_move_type,
        st.operator AS move_operator,
        st.qty AS move_qty
    FROM valid s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        unit_price_distance,
        move_id,
        move_unit_price,
        move_move_type,
        move_operator,
        move_qty,
        LEAD(moved_at, 3) OVER (PARTITION BY move_move_type, move_type ORDER BY moved_at, id, move_id) AS next3_moved_at,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY warehouse_id)) AS distinct_sku_id_in_warehouse_id,
        FIRST_VALUE(move_qty) OVER (ORDER BY unit_price_distance DESC, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_move_qty,
        ROW_NUMBER() OVER (PARTITION BY move_move_type, move_type ORDER BY moved_at, id, move_id) AS rn,
        SIZE(COLLECT_SET(move_move_type) OVER (PARTITION BY warehouse_id)) AS distinct_move_move_type_in_warehouse_id
    FROM with_children_moves
)
SELECT
    warehouse_id,
    distinct_sku_id_in_warehouse_id,
    GROUPING_ID() AS gid,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(distinct_move_move_type_in_warehouse_id, 0.5) AS median_distinct_move_move_type_in_warehouse_id,
    COUNT(DISTINCT sku_id) AS distinct_sku_ids
FROM ranked
GROUP BY GROUPING SETS ((warehouse_id, distinct_sku_id_in_warehouse_id), (warehouse_id))
ORDER BY warehouse_id NULLS LAST, distinct_sku_id_in_warehouse_id NULLS LAST
