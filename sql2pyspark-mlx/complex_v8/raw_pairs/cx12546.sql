WITH valid_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix
    FROM stock_moves
    WHERE (unit_price > 150.7 OR qty > 90)
),
move_type_stats AS (
    -- Median unit_price per move_type
    SELECT move_type, PERCENTILE_APPROX(unit_price, 0.5) AS median_unit_price
    FROM valid_moves
    GROUP BY move_type
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_unit_price,
        ROUND(s.unit_price - g.median_unit_price, 2) AS unit_price_minus_median
    FROM valid_moves s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.unit_price >= g.median_unit_price
),
with_parent_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator_prefix,
        s.unit_price_minus_median,
        sk.weight_kg AS sku_weight_kg
    FROM compared s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM with_parent_sku s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.qty < 384
        )
      AND s.unit_price_minus_median > -150.0
)
SELECT
    move_type,
    warehouse_id,
    MAX(unit_price_minus_median) AS max_unit_price_minus_median,
    ROUND(SUM(unit_price_minus_median) / NULLIF(SUM(qty), 0), 3) AS unit_price_minus_median_to_qty_ratio,
    COUNT(DISTINCT id) AS distinct_ids
FROM screened
GROUP BY GROUPING SETS ((move_type, warehouse_id), (move_type), (warehouse_id), ())
ORDER BY move_type NULLS LAST, warehouse_id NULLS LAST
