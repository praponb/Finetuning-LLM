WITH recent AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        unit_cost,
        labels
    FROM skus
    WHERE unit_cost > 38.3
)
SELECT
    'sku' AS origin,
    id AS qty,
    sku_code,
    unit_cost
FROM recent
UNION ALL
SELECT
    'move' AS origin,
    id,
    move_type,
    unit_price
FROM stock_moves
