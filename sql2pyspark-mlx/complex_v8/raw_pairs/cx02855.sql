WITH base AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label,
        CASE WHEN qty >= 314 THEN 'A' WHEN qty >= 145 THEN 'B' ELSE 'C' END AS qty_tier
    FROM stock_moves
    WHERE TO_DATE(moved_at) < DATE '2025-02-24'
),
move_type_stats AS (
    -- Median unit_price per move_type
    SELECT move_type, PERCENTILE_APPROX(unit_price, 0.5) AS median_unit_price
    FROM base
    GROUP BY move_type
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_unit_price,
        ROUND(s.unit_price - g.median_unit_price, 2) AS unit_price_minus_median
    FROM base s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.unit_price >= g.median_unit_price
),
joined_warehouse AS (
    -- Enrich with dimension data
    SELECT s.*, w.opened_on AS warehouse_opened_on
    FROM compared s
    JOIN warehouses w ON w.id = s.warehouse_id
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        move_type,
        operator,
        unit_price,
        qty_tier,
        median_unit_price,
        unit_price_minus_median,
        ROUND(unit_price_minus_median / NULLIF(unit_price, 0), 3) AS unit_price_minus_median_per_unit_price
    FROM joined_warehouse
),
combined_details AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.operator,
        s.unit_price,
        s.qty_tier,
        s.median_unit_price,
        s.unit_price_minus_median_per_unit_price,
        w.capacity AS warehouse_capacity,
        w.city AS warehouse_city
    FROM scored s
    JOIN warehouses w ON w.id = s.warehouse_id
),
with_parent AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.operator,
        s.unit_price,
        s.qty_tier,
        s.median_unit_price,
        s.unit_price_minus_median_per_unit_price,
        s.warehouse_capacity,
        s.warehouse_city,
        w.opened_on AS warehouse_opened_on
    FROM combined_details s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
)
SELECT id, metric, value
FROM (
    SELECT id, median_unit_price, unit_price_minus_median_per_unit_price
    FROM with_parent
) src
UNPIVOT (
    value FOR metric IN (median_unit_price, unit_price_minus_median_per_unit_price)
) unp
ORDER BY id, metric
