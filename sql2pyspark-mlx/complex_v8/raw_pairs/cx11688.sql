WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        LEAST(unit_price, qty) AS least_unit_price_qty,
        FLOOR(qty / 100) * 100 AS qty_bucket,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd HH:00') AS moved_at_label
    FROM stock_moves
    WHERE (operator NOT IN ('op_ann', 'op_dew') OR warehouse_id IS NOT NULL)
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(moved_at_label, '#', CAST(sku_id AS STRING)) AS moved_at_label_ref,
        ROUND(least_unit_price_qty * 0.9, 2) AS least_unit_price_qty_adj,
        CONCAT_WS('|', move_type, operator) AS move_type_operator_label
    FROM filtered
),
latest AS (
    -- Keep the latest row per sku_id, operator
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        least_unit_price_qty,
        qty_bucket,
        moved_at_label,
        moved_at_label_ref,
        least_unit_price_qty_adj,
        move_type_operator_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id, operator ORDER BY qty_bucket DESC, id DESC) AS row_num
        FROM scored
    ) ranked
    WHERE row_num = 1
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, MAX(qty) AS max_move_qty, COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY warehouse_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.max_move_qty,
        COALESCE(st.move_count, 0) AS move_count
    FROM latest s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN move_count >= 6 THEN 'HIGH' WHEN move_count >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS move_count_tier,
        ROUND(move_count / NULLIF(qty_bucket, 0), 3) AS move_count_per_qty_bucket
    FROM with_counts
    WHERE moved_at < TIMESTAMP '2025-03-08 09:00:00'
)
SELECT *
FROM (
    SELECT moved_at, operator, least_unit_price_qty_adj
    FROM enriched
) src
PIVOT (
    AVG(least_unit_price_qty_adj)
    FOR operator IN ('op_eve', 'op_dew')
)
ORDER BY moved_at
