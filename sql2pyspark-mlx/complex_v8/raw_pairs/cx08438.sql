WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label,
        FLOOR(qty / 100) * 100 AS qty_bucket
    FROM stock_moves
    WHERE move_type <> 'ADJUST'
      AND sku_id >= 16
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_moved_at_month_by_unit_price, 3) AS unit_price_cume_dist,
        MAX(unit_price) OVER (ORDER BY qty DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS unit_price_max_4,
        COUNT(*) OVER w_moved_at_month AS rows_in_moved_at_month
    FROM prepared
    WINDOW w_moved_at_month_by_unit_price AS (PARTITION BY DATE_TRUNC('MONTH', moved_at) ORDER BY unit_price DESC), w_moved_at_month AS (PARTITION BY DATE_TRUNC('MONTH', moved_at))
),
filtered AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM windowed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.operator = 'op_chan'
        )
),
rollup_base AS (
    -- Summarise per group
    SELECT
        move_type,
        sku_id,
        MIN(qty_bucket) AS min_qty_bucket,
        ROUND(SUM(unit_price), 2) AS total_unit_price,
        PERCENTILE_APPROX(unit_price_max_4, 0.5) AS median_unit_price_max_4
    FROM filtered
    GROUP BY 1, 2
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        move_type,
        sku_id,
        min_qty_bucket,
        total_unit_price,
        median_unit_price_max_4,
        ROUND(AVG(min_qty_bucket) OVER (PARTITION BY sku_id), 2) AS sku_id_avg_min_qty_bucket,
        ROUND(AVG(median_unit_price_max_4) OVER (PARTITION BY move_type ORDER BY total_unit_price, move_type, sku_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS median_unit_price_max_4_avg_6_prior
    FROM rollup_base
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        sk.sku_code,
        sk.category AS sku_category,
        sk.unit_cost AS sku_unit_cost
    FROM with_trends s
    JOIN skus sk ON sk.id = s.sku_id
)
-- Subtotals with ROLLUP
SELECT
    sku_id,
    COALESCE(sku_category, 'ALL') AS sku_category,
    GROUPING(sku_id) AS is_total_sku_id,
    COUNT(DISTINCT sku_id) AS distinct_sku_ids
FROM combined_details
GROUP BY ROLLUP(sku_id, sku_category)
