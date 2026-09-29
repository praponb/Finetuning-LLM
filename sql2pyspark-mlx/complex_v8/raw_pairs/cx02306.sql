WITH recent_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
      AND unit_price >= 160.8
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM recent_moves s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.operator NOT IN ('op_ann', 'op_eve')
        )
      AND s.operator = 'op_dew'
),
qualified_2 AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM qualified s
    WHERE s.sku_id IN (
            SELECT st.sku_id
            FROM stock_moves st
            WHERE st.operator IN ('op_chan', 'op_dew')
        )
      AND s.unit_price > 160.8
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(3) OVER w_operator_warehouse_id_by_uni AS unit_price_tercile,
        ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY unit_price_per_qty, id) AS unit_price_per_qty_seq,
        COUNT(*) OVER w_operator AS rows_in_operator
    FROM qualified_2
    WINDOW w_operator_warehouse_id_by_uni AS (PARTITION BY operator, warehouse_id ORDER BY unit_price, id), w_operator AS (PARTITION BY operator)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        unit_price_tercile - LAG(unit_price_tercile) OVER (PARTITION BY sku_id, unit_price_per_qty_seq ORDER BY unit_price DESC, id) AS unit_price_tercile_change,
        RANK() OVER (PARTITION BY sku_id, unit_price_per_qty_seq ORDER BY unit_price DESC) AS unit_price_rank,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY sku_id, unit_price_tercile)) AS distinct_warehouse_id_in_sku_id_unit_price_terci,
        LAST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY sku_id, unit_price_per_qty_seq ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY sku_id, unit_price_tercile)) AS distinct_operator_in_sku_id_unit_price_tercile,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY sku_id, unit_price_tercile), 2) AS unit_price_vs_sku_id_unit_price_tercile_avg
    FROM windowed
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN st.operator <> 'op_chan' THEN 1 END) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_operator_count,
        (SELECT COUNT(*) FILTER (WHERE st.operator = 'op_bee') FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_operator_count_2,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at
    FROM sequenced s
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, amount
FROM (
    SELECT id, unit_price, unit_price_vs_sku_id_unit_price_tercile_avg, unit_price_per_qty
    FROM profiled
) src
UNPIVOT (
    amount FOR field IN (unit_price, unit_price_vs_sku_id_unit_price_tercile_avg, unit_price_per_qty)
) unp
