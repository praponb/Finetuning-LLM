WITH base AS (
    -- Filter stock_moves
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
    WHERE sku_id IS NOT NULL
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        TO_DATE(moved_at) AS moved_at_date,
        CASE WHEN unit_price >= 298.9 THEN 'large' WHEN unit_price >= 150.7 THEN 'medium' ELSE 'small' END AS unit_price_tier
    FROM base
),
move_type_stats AS (
    -- Avg unit_price per move_type
    SELECT move_type, AVG(unit_price) AS avg_unit_price
    FROM scored
    GROUP BY move_type
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_unit_price,
        ROUND(s.unit_price - g.avg_unit_price, 2) AS unit_price_minus_avg
    FROM scored s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.unit_price > g.avg_unit_price
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, metric_value
FROM (
    SELECT id, avg_unit_price, unit_price_minus_avg, unit_price
    FROM outliers
) src
UNPIVOT (
    metric_value FOR metric IN (avg_unit_price, unit_price_minus_avg, unit_price)
) unp
ORDER BY id, metric
