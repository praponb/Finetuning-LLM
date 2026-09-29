WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        LEAST(unit_price, qty) AS least_unit_price_qty,
        DATE_TRUNC('YEAR', moved_at) AS moved_at_year
    FROM stock_moves
    WHERE sku_id <= 13
),
operator_stats AS (
    -- Median unit_price per operator
    SELECT operator, PERCENTILE_APPROX(unit_price, 0.5) AS median_unit_price
    FROM recent
    GROUP BY operator
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_unit_price,
        ROUND(s.unit_price - g.median_unit_price, 2) AS unit_price_minus_median
    FROM recent s
    JOIN operator_stats g ON g.operator = s.operator
    WHERE s.unit_price >= g.median_unit_price
),
with_parent_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        w.opened_on AS warehouse_opened_on,
        w.capacity AS warehouse_capacity,
        w.city AS warehouse_city
    FROM outliers s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id AND w.city = 'Kuala Lumpur'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT moved_at_year, move_type, warehouse_city, warehouse_capacity
    FROM with_parent_warehouse
) src
PIVOT (
    SUM(warehouse_capacity)
    FOR warehouse_city IN ('Bangkok', 'Singapore')
)
