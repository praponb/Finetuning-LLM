WITH valid AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        CASE WHEN unit_price >= 361.9 THEN 'HIGH' WHEN unit_price >= 206.8 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_tier
    FROM stock_moves
    WHERE move_type NOT IN ('ADJUST', 'INBOUND')
)
SELECT id AS entity_id, unit_price_tier AS label
FROM valid
WHERE warehouse_id IS NOT NULL
UNION
SELECT id AS entity_id, move_type AS label
FROM stock_moves
ORDER BY label DESC, entity_id
