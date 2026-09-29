WITH valid AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        SUBSTRING(TRIM(move_type), 1, 3) AS move_type_prefix
    FROM stock_moves
)
-- Final projection
SELECT
    id,
    sku_id,
    move_type,
    operator,
    unit_price,
    move_type_prefix,
    COALESCE(unit_price, 0.0) AS unit_price_filled
FROM valid
ORDER BY unit_price, id
LIMIT 5
