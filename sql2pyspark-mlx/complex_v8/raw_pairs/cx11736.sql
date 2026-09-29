WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        DATE_TRUNC('HOUR', moved_at) AS moved_at_hour,
        CONCAT(move_type, '#', CAST(qty AS STRING)) AS move_type_ref,
        LEAST(unit_price, qty) AS least_unit_price_qty
    FROM stock_moves
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        sku_id,
        move_type,
        operator,
        move_type_ref,
        least_unit_price_qty,
        GREATEST(least_unit_price_qty, unit_price) AS greatest_least_unit_price_qty_unit_price
    FROM filtered
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        sku_id,
        move_type,
        move_type_ref,
        least_unit_price_qty,
        greatest_least_unit_price_qty_unit_price,
        FLOOR(least_unit_price_qty / 100) * 100 AS least_unit_price_qty_bucket
    FROM annotated
    WHERE move_type = 'TRANSFER'
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        MAX(moved_at) AS last_moved_at,
        MAX(moved_at) AS last_moved_at_2,
        COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY sku_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.last_moved_at,
        st.last_moved_at_2,
        COALESCE(st.move_count, 0) AS move_count
    FROM enriched s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
combined_details_sku AS (
    -- Join skus
    SELECT s.*, sk.is_active AS sku_is_active
    FROM summarized_rows s
    JOIN skus sk ON sk.id = s.sku_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_id,
        move_type_ref,
        greatest_least_unit_price_qty_unit_price,
        least_unit_price_qty_bucket,
        last_moved_at,
        move_count,
        LAG(last_moved_at_2) OVER (PARTITION BY move_count ORDER BY least_unit_price_qty_bucket DESC NULLS FIRST, id) AS prev_last_moved_at_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY sku_is_active ORDER BY least_unit_price_qty DESC), 3) AS least_unit_price_qty_percent_rank,
        DENSE_RANK() OVER (PARTITION BY sku_is_active ORDER BY least_unit_price_qty DESC) AS least_unit_price_qty_dense_rank
    FROM combined_details_sku
),
summary AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', last_moved_at) AS last_moved_at_month,
        least_unit_price_qty_dense_rank,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids,
        ROUND(AVG(least_unit_price_qty_percent_rank), 2) AS avg_least_unit_price_qty_percent_rank,
        MAX(prev_last_moved_at_2) AS last_prev_last_moved_at_2,
        MIN(greatest_least_unit_price_qty_unit_price) AS min_greatest_least_unit_price_qty_unit_price
    FROM metrics
    WHERE least_unit_price_qty_dense_rank > 2
    GROUP BY 1, 2
    HAVING MAX(greatest_least_unit_price_qty_unit_price) > 100.8
)
-- Final projection
SELECT
    last_moved_at_month,
    distinct_sku_ids,
    avg_least_unit_price_qty_percent_rank,
    last_prev_last_moved_at_2,
    DATE_FORMAT(last_moved_at_month, 'yyyy-MM-dd HH:00') AS last_moved_at_month_label
FROM summary
ORDER BY avg_least_unit_price_qty_percent_rank DESC, last_moved_at_month
