WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price,
        INITCAP(TRIM(move_type)) AS move_type_clean
    FROM stock_moves
    WHERE MONTH(moved_at) IN (1, 3)
      AND operator IS NOT NULL
),
move_type_stats AS (
    -- Avg unit_price per move_type
    SELECT move_type, AVG(unit_price) AS avg_unit_price
    FROM base_moves
    GROUP BY move_type
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_unit_price,
        ROUND(s.unit_price - g.avg_unit_price, 2) AS unit_price_minus_avg
    FROM base_moves s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.unit_price > g.avg_unit_price
)
SELECT *
FROM (
    SELECT warehouse_id, move_type, avg_unit_price
    FROM compared
) src
PIVOT (
    MAX(avg_unit_price)
    FOR move_type IN ('ADJUST', 'TRANSFER', 'INBOUND', 'OUTBOUND')
)
ORDER BY warehouse_id
