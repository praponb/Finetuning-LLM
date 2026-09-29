WITH cleaned_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
),
scored AS (
    -- Add calculated columns
    SELECT *, COALESCE(unit_price, 0.0) AS unit_price_filled
    FROM cleaned_moves
    WHERE unit_price >= 61.2
),
with_details_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price_filled,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on
    FROM scored s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(unit_price_filled, 0.0) AS unit_price_filled_filled,
        CASE WHEN unit_price_filled >= 350.3 THEN 'HIGH' WHEN unit_price_filled >= 200.6 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_filled_tier
    FROM with_details_warehouse
    WHERE move_type NOT IN ('OUTBOUND', 'ADJUST')
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    unit_price_filled,
    warehouse_wh_code,
    warehouse_opened_on,
    unit_price_filled_filled,
    unit_price_filled_tier,
    MONTH(moved_at) AS moved_at_month
FROM labeled
ORDER BY unit_price_filled DESC, sku_id NULLS LAST
