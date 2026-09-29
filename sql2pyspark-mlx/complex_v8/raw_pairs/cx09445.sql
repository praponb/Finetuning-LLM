WITH scoped_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        CASE WHEN operator IN ('op_chan', 'op_dew') THEN 'group_a' WHEN operator = 'op_eve' THEN 'group_b' ELSE 'other' END AS operator_group
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    operator,
    unit_price,
    operator_group,
    LENGTH(TRIM(operator)) AS operator_len,
    LOWER(TRIM(operator_group)) AS operator_group_clean
FROM scoped_moves
ORDER BY move_type DESC, sku_id DESC
