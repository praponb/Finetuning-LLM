WITH scoped AS (
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
        ROUND(unit_price * 1.5, 2) AS unit_price_adj
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
line_level_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.unit_price_adj,
        st.id AS move_id,
        st.qty AS move_qty,
        st.warehouse_id AS move_warehouse_id,
        st.unit_price AS move_unit_price
    FROM scoped s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_id,
        qty,
        operator,
        unit_price_adj,
        move_id,
        move_qty,
        move_warehouse_id,
        move_unit_price,
        move_unit_price - LAG(move_unit_price) OVER w_warehouse_id_sku_id_by_unit AS move_unit_price_change,
        LEAD(unit_price_adj) OVER (PARTITION BY warehouse_id, move_id ORDER BY move_qty, id, move_id) AS next_unit_price_adj
    FROM line_level_moves
    WINDOW w_warehouse_id_sku_id_by_unit AS (PARTITION BY warehouse_id, sku_id ORDER BY unit_price_adj DESC, id, move_id)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN next_unit_price_adj >= 562.8 THEN 'large' WHEN next_unit_price_adj >= 263.1 THEN 'medium' ELSE 'small' END AS next_unit_price_adj_tier,
        LENGTH(TRIM(operator)) AS operator_len
    FROM metrics
    WHERE move_unit_price_change >= -350.0
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.sku_id,
        s.qty,
        s.operator,
        s.move_id,
        s.move_warehouse_id,
        s.move_unit_price_change,
        s.next_unit_price_adj,
        s.next_unit_price_adj_tier,
        st.id AS move_id_2,
        st.unit_price AS move_unit_price_2,
        st.operator AS move_operator,
        st.move_type
    FROM tagged s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(next_unit_price_adj) OVER w_operator_move_warehouse_id AS operator_move_warehouse_id_max_next_unit_price_a,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY operator, move_warehouse_id), 0), 4) AS qty_share_of_operator_move_warehouse_id,
        ROUND(move_unit_price_2 / NULLIF(SUM(move_unit_price_2) OVER (PARTITION BY operator, move_warehouse_id), 0), 4) AS move_unit_price_2_share_of_operator_move_warehou,
        ROUND(AVG(next_unit_price_adj) OVER (PARTITION BY move_id_2 ORDER BY qty ASC, id, move_id, move_id_2 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS next_unit_price_adj_avg_2,
        ROUND(AVG(qty) OVER (PARTITION BY move_id_2 ORDER BY qty ASC, id, move_id, move_id_2 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS qty_avg_3,
        SIZE(COLLECT_SET(move_id) OVER (PARTITION BY operator, move_warehouse_id)) AS distinct_move_id_in_operator_move_warehouse_id
    FROM with_children_moves
    WINDOW w_operator_move_warehouse_id AS (PARTITION BY operator, move_warehouse_id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT move_id, next_unit_price_adj_tier, move_operator, move_unit_price_2_share_of_operator_move_warehou
    FROM ranked
) src
PIVOT (
    SUM(move_unit_price_2_share_of_operator_move_warehou)
    FOR move_operator IN ('op_bee', 'op_dew', 'op_eve', 'op_ann')
)
