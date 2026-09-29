WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-02-18 15:00:00'
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN unit_price >= 341.5 THEN 'A' WHEN unit_price >= 160.8 THEN 'B' ELSE 'C' END AS unit_price_tier,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty
    FROM cleaned
    WHERE warehouse_id IS NOT NULL
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(unit_price_per_qty, 0.0) AS unit_price_per_qty_filled,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at,
        ABS(unit_price - 236.5) AS unit_price_distance
    FROM annotated
    WHERE moved_at < TIMESTAMP '2025-03-08 09:00:00'
)
SELECT *
FROM (
    SELECT warehouse_id, move_type, unit_price_per_qty
    FROM tagged
) src
PIVOT (
    SUM(unit_price_per_qty)
    FOR move_type IN ('TRANSFER', 'INBOUND', 'ADJUST', 'OUTBOUND')
)
ORDER BY warehouse_id
