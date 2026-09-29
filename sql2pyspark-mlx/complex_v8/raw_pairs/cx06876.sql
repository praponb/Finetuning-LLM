WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        CONCAT_WS('-', move_type, operator) AS move_type_operator_label,
        UPPER(TRIM(operator)) AS operator_clean,
        CONCAT_WS('|', operator, move_type) AS operator_move_type_label
    FROM stock_moves
    WHERE sku_id IS NOT NULL
      AND unit_price >= 111.7
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        operator_move_type_label,
        CONCAT(operator_move_type_label, '#', CAST(id AS STRING)) AS operator_move_type_label_ref,
        CASE WHEN unit_price >= 361.9 THEN 'HIGH' WHEN unit_price >= 164.1 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_tier,
        LENGTH(TRIM(move_type_operator_label)) AS move_type_operator_label_len
    FROM filtered
    WHERE operator_move_type_label IS NOT NULL
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM scored s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE st.qty <= 349
        )
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM with_activity s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
        )
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        move_type,
        operator_move_type_label,
        operator_move_type_label_ref,
        unit_price_tier,
        move_type_operator_label_len,
        CONCAT(unit_price_tier, '#', CAST(sku_id AS STRING)) AS unit_price_tier_ref
    FROM without_matches
    WHERE warehouse_id <= 2
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count,
        (SELECT ROUND(AVG(st.qty), 2) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS avg_move_qty
    FROM annotated s
)
SELECT id, metric_name, value
FROM (
    SELECT id, move_count, move_type_operator_label_len
    FROM summarized_rows
) src
UNPIVOT (
    value FOR metric_name IN (move_count, move_type_operator_label_len)
) unp
