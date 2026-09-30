WITH cleaned_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS(' / ', operator, move_type_slug) AS operator_move_type_slug_label
    FROM cleaned_moves
    WHERE unit_price <= 293.2
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    unit_price,
    move_type_slug,
    operator_move_type_slug_label,
    LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug_2
FROM tagged
WHERE operator = 'op_bee'
ORDER BY sku_id DESC
