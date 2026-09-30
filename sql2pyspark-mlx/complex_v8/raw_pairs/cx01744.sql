WITH scoped_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price
    FROM stock_moves
    WHERE operator = 'op_bee'
),
enriched AS (
    -- Join warehouses
    SELECT
        s.*,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city,
        sk.category AS sku_category,
        sk.sku_code
    FROM scoped_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
operator_stats AS (
    -- Avg unit_price per operator
    SELECT operator, AVG(unit_price) AS avg_unit_price
    FROM enriched
    GROUP BY operator
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_unit_price,
        ROUND(s.unit_price - g.avg_unit_price, 2) AS unit_price_minus_avg
    FROM enriched s
    JOIN operator_stats g ON g.operator = s.operator
    WHERE s.unit_price > g.avg_unit_price
)
SELECT *
FROM (
    SELECT warehouse_id, operator, avg_unit_price
    FROM outliers
) src
PIVOT (
    SUM(avg_unit_price)
    FOR operator IN ('op_eve', 'op_chan', 'op_dew', 'op_bee')
)
ORDER BY warehouse_id
