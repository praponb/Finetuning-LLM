WITH scoped AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        MONTH(moved_at) AS moved_at_month,
        LEAST(unit_price, qty) AS least_unit_price_qty
    FROM stock_moves
    WHERE unit_price < 328.3
      AND sku_id IS NOT NULL
),
enriched AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.unit_price,
        s.least_unit_price_qty,
        w.city AS warehouse_city,
        w.opened_on AS warehouse_opened_on,
        w.wh_code AS warehouse_wh_code
    FROM scoped s
    JOIN warehouses w ON w.id = s.warehouse_id
),
warehouse_city_stats AS (
    -- Avg unit_price per warehouse_city
    SELECT warehouse_city, AVG(unit_price) AS avg_unit_price
    FROM enriched
    GROUP BY warehouse_city
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_unit_price,
        ROUND(s.unit_price - g.avg_unit_price, 2) AS unit_price_minus_avg
    FROM enriched s
    JOIN warehouse_city_stats g ON g.warehouse_city = s.warehouse_city
    WHERE s.unit_price > g.avg_unit_price
)
SELECT
    id,
    warehouse_id,
    sku_id,
    least_unit_price_qty,
    avg_unit_price,
    unit_price_minus_avg
FROM vs_group
ORDER BY least_unit_price_qty
