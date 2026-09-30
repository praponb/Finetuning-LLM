WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        SUBSTRING(TRIM(operator), 1, 2) AS operator_prefix,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd') AS moved_at_label,
        ABS(unit_price - 298.9) AS unit_price_distance
    FROM stock_moves
    WHERE move_type <> 'ADJUST'
      AND warehouse_id >= 2
),
enriched AS (
    -- Join skus
    SELECT
        s.*,
        sk.is_active AS sku_is_active,
        w.capacity AS warehouse_capacity,
        w.opened_on AS warehouse_opened_on
    FROM filtered s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
sku_id_stats AS (
    -- Median unit_price per sku_id
    SELECT sku_id, PERCENTILE_APPROX(unit_price, 0.5) AS median_unit_price
    FROM enriched
    GROUP BY sku_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_unit_price,
        ROUND(s.unit_price - g.median_unit_price, 2) AS unit_price_minus_median
    FROM enriched s
    JOIN sku_id_stats g ON g.sku_id = s.sku_id
    WHERE s.unit_price >= g.median_unit_price
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', warehouse_opened_on) AS warehouse_opened_on_month,
        sku_id,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        ROUND(SUM(unit_price_distance), 2) AS total_unit_price_distance,
        MAX(warehouse_opened_on) AS last_warehouse_opened_on,
        COUNT(*) AS cnt,
        SUM(unit_price_distance) AS total_unit_price_distance_2
    FROM outliers
    GROUP BY DATE_TRUNC('MONTH', warehouse_opened_on), sku_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS last_moved_at,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count,
        (SELECT COUNT(*) FILTER (WHERE st.move_type IN ('ADJUST', 'OUTBOUND', 'TRANSFER')) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_move_type_count
    FROM per_group s
)
-- Final projection
SELECT
    warehouse_opened_on_month,
    sku_id,
    operator_list,
    total_unit_price_distance,
    last_warehouse_opened_on,
    cnt,
    total_unit_price_distance_2,
    last_moved_at,
    move_count,
    move_move_type_count
FROM with_counts
ORDER BY total_unit_price_distance, warehouse_opened_on_month, sku_id
LIMIT 20
