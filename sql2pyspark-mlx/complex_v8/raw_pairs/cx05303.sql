WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-01-13 10:00:00'
),
labeled AS (
    -- Derive helper columns
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
    FROM filtered_moves
),
enriched AS (
    -- Label and score rows
    SELECT *, LEAST(unit_price, qty) AS least_unit_price_qty
    FROM labeled
),
filtered AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND w.opened_on >= DATE '2024-11-25'
              AND w.wh_code IS NOT NULL
        )
)
SELECT
    operator,
    move_type,
    SUM(unit_price_per_qty) AS total_unit_price_per_qty
FROM filtered
GROUP BY CUBE(operator, move_type)
ORDER BY operator NULLS LAST, move_type NULLS LAST
