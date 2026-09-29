WITH filtered_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-02-12 10:00:00'
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM filtered_moves s
    WHERE s.sku_id IN (
            SELECT st.sku_id
            FROM stock_moves st
            WHERE st.qty BETWEEN 99 AND 445
              AND st.unit_price < 330.0
        )
),
operator_stats AS (
    -- Median qty per operator
    SELECT operator, PERCENTILE_APPROX(qty, 0.5) AS median_qty
    FROM qualified
    GROUP BY operator
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_qty, ROUND(s.qty - g.median_qty, 2) AS qty_minus_median
    FROM qualified s
    JOIN operator_stats g ON g.operator = s.operator
    WHERE s.qty >= g.median_qty
),
qualified_2 AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM outliers s
    WHERE s.sku_id IN (
            SELECT st.sku_id
            FROM stock_moves st
        )
      AND s.unit_price <= 363.6
)
SELECT *
FROM (
    SELECT moved_at, warehouse_id, move_type, unit_price
    FROM qualified_2
) src
PIVOT (
    SUM(unit_price)
    FOR move_type IN ('TRANSFER', 'OUTBOUND', 'ADJUST', 'INBOUND')
)
