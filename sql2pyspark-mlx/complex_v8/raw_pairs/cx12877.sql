WITH valid AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        CONCAT_WS('|', move_type, operator) AS move_type_operator_label
    FROM stock_moves
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        move_type_operator_label,
        ABS(qty - 168) AS qty_distance,
        ABS(unit_price - 265.0) AS unit_price_distance
    FROM valid
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM scored
    WHERE qty < (SELECT MAX(qty) FROM scored)
)
SELECT
    operator,
    sku_id,
    move_type,
    GROUPING(operator) AS is_total_operator,
    COUNT(*) FILTER (WHERE move_type = 'TRANSFER') AS n_move_type_transfer,
    ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
    ROUND(STDDEV(qty), 2) AS stddev_qty
FROM qualified
GROUP BY GROUPING SETS ((operator, sku_id, move_type), (operator), (sku_id))
ORDER BY operator NULLS LAST, sku_id NULLS LAST, move_type NULLS LAST
