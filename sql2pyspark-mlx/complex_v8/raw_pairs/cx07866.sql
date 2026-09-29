WITH cleaned AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        LEAST(qty, unit_price) AS least_qty_unit_price
    FROM stock_moves
),
operator_stats AS (
    -- Avg least_qty_unit_price per operator
    SELECT operator, AVG(least_qty_unit_price) AS avg_least_qty_unit_price
    FROM cleaned
    GROUP BY operator
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_least_qty_unit_price,
        ROUND(s.least_qty_unit_price - g.avg_least_qty_unit_price, 2) AS least_qty_unit_price_minus_avg
    FROM cleaned s
    JOIN operator_stats g ON g.operator = s.operator
    WHERE s.least_qty_unit_price > g.avg_least_qty_unit_price
),
move_type_stats AS (
    -- Avg least_qty_unit_price_minus_avg per move_type
    SELECT move_type, AVG(least_qty_unit_price_minus_avg) AS avg_least_qty_unit_price_minus_avg
    FROM outliers
    GROUP BY move_type
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_least_qty_unit_price_minus_avg,
        ROUND(s.least_qty_unit_price_minus_avg - g.avg_least_qty_unit_price_minus_avg, 2) AS least_qty_unit_price_minus_avg_minus_avg
    FROM outliers s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.least_qty_unit_price_minus_avg > g.avg_least_qty_unit_price_minus_avg
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, avg_least_qty_unit_price, avg_least_qty_unit_price_minus_avg, least_qty_unit_price, least_qty_unit_price_minus_avg
    FROM compared
) src
UNPIVOT (
    metric_value FOR metric_name IN (avg_least_qty_unit_price, avg_least_qty_unit_price_minus_avg, least_qty_unit_price, least_qty_unit_price_minus_avg)
) unp
