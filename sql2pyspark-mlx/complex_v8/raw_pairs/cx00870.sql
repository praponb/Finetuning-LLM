WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        CASE WHEN unit_price >= 313.8 THEN 'A' WHEN unit_price >= 136.0 THEN 'B' ELSE 'C' END AS unit_price_tier,
        CONCAT(move_type, '#', CAST(id AS STRING)) AS move_type_ref
    FROM stock_moves
    WHERE (qty BETWEEN 99 AND 458 OR moved_at < TIMESTAMP '2025-03-08 11:00:00')
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        unit_price_tier,
        move_type_ref,
        LEAST(qty, unit_price) AS least_qty_unit_price
    FROM filtered
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price_tier,
        move_type_ref,
        SUM(qty) OVER w_warehouse_id_sku_id AS warehouse_id_sku_id_sum_qty,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY warehouse_id, sku_id)) AS distinct_operator_in_warehouse_id_sku_id,
        MAX(qty) OVER (ORDER BY least_qty_unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_qty_so_far
    FROM derived
    WINDOW w_warehouse_id_sku_id AS (PARTITION BY warehouse_id, sku_id)
),
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM windowed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND TO_DATE(st.moved_at) < DATE '2025-03-02'
        )
),
annotated AS (
    -- Add calculated columns
    SELECT *, GREATEST(qty, max_qty_so_far) AS greatest_qty_max_qty_so_far
    FROM without_matches
    WHERE max_qty_so_far <= 3250
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, amount
FROM (
    SELECT id, distinct_operator_in_warehouse_id_sku_id, qty, max_qty_so_far, greatest_qty_max_qty_so_far
    FROM annotated
) src
UNPIVOT INCLUDE NULLS (
    amount FOR metric IN (distinct_operator_in_warehouse_id_sku_id AS distinct_operator_in_warehouse_id_sku_id_v, qty AS qty_v, max_qty_so_far AS m_max_qty_so_far, greatest_qty_max_qty_so_far AS m_greatest_qty_max_qty_so_far)
) unp
