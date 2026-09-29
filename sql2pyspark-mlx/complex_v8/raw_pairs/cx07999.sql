WITH prepared_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown,
        CONCAT_WS('|', move_type, operator) AS move_type_operator_label,
        ABS(qty - 256) AS qty_distance
    FROM stock_moves
    WHERE operator NOT IN ('op_ann', 'op_dew')
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(move_type)) AS move_type_len,
        COALESCE(qty_distance, 0) AS qty_distance_filled,
        COALESCE(qty, 0) AS qty_filled
    FROM prepared_moves
    WHERE move_type = 'TRANSFER'
)
-- Subtotals with GROUPING SETS
SELECT
    move_type_len,
    move_type,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(operator)) AS operator_list
FROM tagged
GROUP BY GROUPING SETS ((move_type_len, move_type), (move_type_len), (move_type), ())
ORDER BY move_type_len NULLS LAST, move_type NULLS LAST
