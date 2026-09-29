WITH scoped_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        COALESCE(unit_price, 0.0) AS unit_price_filled,
        CASE WHEN qty >= 372 THEN 'HIGH' WHEN qty >= 198 THEN 'MEDIUM' ELSE 'LOW' END AS qty_tier,
        ROUND(unit_price * 0.9, 2) AS unit_price_adj
    FROM stock_moves
    WHERE (sku_id IS NOT NULL OR moved_at >= TIMESTAMP '2025-02-06 08:00:00')
),
enriched AS (
    -- Join warehouses
    SELECT s.*, w.opened_on AS warehouse_opened_on, w.city AS warehouse_city
    FROM scoped_moves s
    JOIN warehouses w ON w.id = s.warehouse_id AND w.city NOT IN ('Bangkok', 'Kuala Lumpur')
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('QUARTER', warehouse_opened_on) AS warehouse_opened_on_quarter,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
    FROM enriched
)
-- Subtotals with GROUPING SETS
SELECT
    move_type,
    operator,
    GROUPING_ID() AS gid,
    COUNT(*) FILTER (WHERE move_type IN ('OUTBOUND', 'ADJUST', 'TRANSFER')) AS n_move_type_in_outbound_adjust
FROM tagged
GROUP BY GROUPING SETS ((move_type, operator), (move_type), ())
ORDER BY move_type NULLS LAST, operator NULLS LAST
