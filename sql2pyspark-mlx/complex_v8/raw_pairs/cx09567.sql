WITH scoped_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
warehouse_id_stats AS (
    -- Max unit_price per warehouse_id
    SELECT warehouse_id, MAX(unit_price) AS max_unit_price
    FROM scoped_moves
    GROUP BY warehouse_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_unit_price,
        ROUND(s.unit_price - g.max_unit_price, 2) AS unit_price_minus_max
    FROM scoped_moves s
    JOIN warehouse_id_stats g ON g.warehouse_id = s.warehouse_id
    WHERE s.unit_price = g.max_unit_price
),
combined_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.max_unit_price,
        s.unit_price_minus_max,
        sk.labels AS sku_labels,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on
    FROM above_average s
    JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
),
move_type_stats AS (
    -- Avg unit_price_minus_max per move_type
    SELECT move_type, AVG(unit_price_minus_max) AS avg_unit_price_minus_max
    FROM combined_details
    GROUP BY move_type
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_unit_price_minus_max,
        ROUND(s.unit_price_minus_max - g.avg_unit_price_minus_max, 2) AS unit_price_minus_max_minus_avg
    FROM combined_details s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.unit_price_minus_max > g.avg_unit_price_minus_max
)
-- Subtotals with ROLLUP
SELECT
    move_type,
    operator,
    sku_id,
    GROUPING_ID() AS gid,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE operator IS NOT NULL) AS n_operator_is_not_null
FROM outliers
GROUP BY ROLLUP(move_type, operator, sku_id)
ORDER BY move_type NULLS LAST, operator NULLS LAST, sku_id NULLS LAST
