WITH prepared AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
),
deduped AS (
    -- Keep the latest row per sku_id, move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id, move_type ORDER BY qty DESC, id DESC) AS row_num
        FROM prepared
    ) ranked
    WHERE row_num = 1
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(qty - 169) AS qty_distance,
        COALESCE(unit_price, 0.0) AS unit_price_filled
    FROM deduped
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    unit_price_filled,
    ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price
FROM derived
WHERE sku_id >= 13
ORDER BY warehouse_id NULLS LAST, unit_price_filled NULLS LAST
