WITH base_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        SUBSTRING(TRIM(move_type), 1, 3) AS move_type_prefix,
        CASE WHEN move_type IN ('INBOUND', 'ADJUST') THEN 'group_a' WHEN move_type = 'OUTBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group,
        ROUND(unit_price * 1.1, 2) AS unit_price_adj
    FROM stock_moves
    WHERE unit_price > 125.9
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price_adj,
        MIN(unit_price_adj) OVER w_operator AS operator_min_unit_price_adj
    FROM base_moves
    WINDOW w_operator AS (PARTITION BY operator)
)
SELECT
    move_type,
    sku_id,
    COUNT(*) AS row_count,
    ARRAY_SORT(COLLECT_SET(operator)) AS operator_list
FROM ranked
GROUP BY ROLLUP(move_type, sku_id)
ORDER BY move_type NULLS LAST, sku_id NULLS LAST
