WITH base AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        unit_price - qty AS unit_price_minus_qty,
        CASE WHEN qty >= 372 THEN 'large' WHEN qty >= 116 THEN 'medium' ELSE 'small' END AS qty_tier
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-02-24 12:00:00'
      AND operator IS NOT NULL
),
combined_details_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.unit_price,
        s.unit_price_minus_qty,
        s.qty_tier,
        w.opened_on AS warehouse_opened_on,
        w.capacity AS warehouse_capacity,
        w.city AS warehouse_city
    FROM base s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(unit_price * 1.5, 2) AS unit_price_adj,
        CONCAT(move_type, '#', CAST(warehouse_capacity AS STRING)) AS move_type_ref,
        LOWER(REGEXP_REPLACE(TRIM(warehouse_city), ' ', '_')) AS warehouse_city_slug
    FROM combined_details_warehouse
    WHERE warehouse_id >= 2
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        qty_tier,
        YEAR(warehouse_opened_on) AS warehouse_opened_on_year,
        sku_id,
        MAX(moved_at) AS last_moved_at,
        COUNT(*) FILTER (WHERE warehouse_city IN ('Bangkok', 'Singapore')) AS n_warehouse_city_in_bangkok_si,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids
    FROM annotated
    GROUP BY 1, 2, 3
)
-- Native SQL UNPIVOT (wide to long)
SELECT qty_tier, warehouse_opened_on_year, field, value
FROM (
    SELECT qty_tier, warehouse_opened_on_year, n_warehouse_city_in_bangkok_si, n_rows
    FROM per_group
) src
UNPIVOT (
    value FOR field IN (n_warehouse_city_in_bangkok_si, n_rows)
) unp
ORDER BY qty_tier, warehouse_opened_on_year, field
