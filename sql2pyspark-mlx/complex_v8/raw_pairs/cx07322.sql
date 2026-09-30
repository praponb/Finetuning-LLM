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
        CASE WHEN operator IN ('op_eve', 'op_ann') THEN 'group_a' WHEN operator = 'op_bee' THEN 'group_b' ELSE 'other' END AS operator_group,
        moved_at + INTERVAL 2 HOURS AS moved_at_plus_2h
    FROM stock_moves
    WHERE TO_DATE(moved_at) < DATE '2025-02-24'
      AND operator = 'op_ann'
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(unit_price * 0.9, 2) AS unit_price_adj,
        DATE_TRUNC('HOUR', moved_at) AS moved_at_hour
    FROM recent
    WHERE unit_price < 387.1
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.operator_group,
        s.unit_price_adj,
        st.id AS move_id,
        st.operator AS move_operator,
        st.move_type AS move_move_type
    FROM labeled s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id AND st.move_type = 'ADJUST'
),
labeled_2 AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(unit_price_adj * 1.5, 2) AS unit_price_adj_adj,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown,
        CASE WHEN move_operator IN ('op_dew', 'op_ann') THEN 'group_a' WHEN move_operator = 'op_bee' THEN 'group_b' ELSE 'other' END AS move_operator_group
    FROM with_children_moves
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT SUM(st.unit_price) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS total_move_unit_price,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at
    FROM labeled_2 s
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM summarized_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.unit_price < 323.9
              AND st.warehouse_id >= 4
        )
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    operator,
    unit_price_adj,
    move_operator,
    move_move_type,
    move_type_or_unknown,
    total_move_unit_price,
    CASE WHEN move_type_or_unknown IN ('TRANSFER', 'INBOUND') THEN 'group_a' WHEN move_type_or_unknown = 'UNKNOWN' THEN 'group_b' ELSE 'other' END AS move_type_or_unknown_group,
    CONCAT(move_operator_group, '#', CAST(sku_id AS STRING)) AS move_operator_group_ref
FROM qualified
WHERE last_moved_at < TIMESTAMP '2025-02-09 09:00:00'
