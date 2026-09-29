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
        CONCAT_WS(': ', move_type, operator) AS move_type_operator_label,
        CONCAT(operator, '#', CAST(qty AS STRING)) AS operator_ref
    FROM stock_moves
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, MIN(moved_at) AS first_moved_at
    FROM stock_moves
    GROUP BY sku_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.first_moved_at
    FROM recent s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM profiled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.qty < 426
        )
      AND s.moved_at >= TIMESTAMP '2025-01-31 10:00:00'
),
stats AS (
    -- Summarise per group
    SELECT
        sku_id,
        warehouse_id,
        ROUND(100 * COUNT(CASE WHEN operator = 'op_bee' THEN 1 END) / COUNT(*), 1) AS pct_operator_op_bee,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        SUM(CASE WHEN operator = 'op_bee' THEN 1 ELSE 0 END) AS n_operator_op_bee,
        COUNT(DISTINCT move_type) AS distinct_move_types,
        SUM(CASE WHEN operator IN ('op_ann', 'op_eve') THEN 1 ELSE 0 END) AS n_operator_in_op_ann_op_eve
    FROM qualified
    GROUP BY sku_id, warehouse_id
    HAVING COUNT(*) >= 2
),
qualified_2 AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM stats s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.unit_price < 396.4
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT sku_id, warehouse_id, field, value
FROM (
    SELECT sku_id, warehouse_id, n_operator_op_bee, n_operator_in_op_ann_op_eve, distinct_move_types
    FROM qualified_2
) src
UNPIVOT (
    value FOR field IN (n_operator_op_bee, n_operator_in_op_ann_op_eve, distinct_move_types)
) unp
