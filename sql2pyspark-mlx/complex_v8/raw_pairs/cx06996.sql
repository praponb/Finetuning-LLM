WITH recent AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM stock_moves
    WHERE move_type IN ('OUTBOUND', 'ADJUST', 'TRANSFER')
),
enriched_warehouse AS (
    -- Enrich with dimension data
    SELECT s.*, w.wh_code AS warehouse_wh_code
    FROM recent s
    JOIN warehouses w ON w.id = s.warehouse_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(unit_price_bucket) OVER (PARTITION BY operator ORDER BY days_since_moved_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS unit_price_bucket_sum_6_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_id ORDER BY days_since_moved_at DESC), 3) AS days_since_moved_at_cume_dist,
        SIZE(COLLECT_SET(warehouse_id) OVER w_move_type_operator) AS distinct_warehouse_id_in_move_type_operator,
        SIZE(COLLECT_SET(sku_id) OVER w_move_type_operator) AS distinct_sku_id_in_move_type_operator,
        COUNT(*) OVER (PARTITION BY move_type, operator) AS rows_in_move_type_operator,
        FIRST_VALUE(days_since_moved_at) IGNORE NULLS OVER (ORDER BY days_since_moved_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_since_moved_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY days_since_moved_at NULLS LAST, id), 3) AS days_since_moved_at_percent_rank,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_move_type_operator, 0), 4) AS unit_price_share_of_move_type_operator,
        ROUND(unit_price_bucket / NULLIF(SUM(unit_price_bucket) OVER w_move_type_operator, 0), 4) AS unit_price_bucket_share_of_move_type_operator,
        ROUND(days_since_moved_at - AVG(days_since_moved_at) OVER (PARTITION BY move_type, operator), 2) AS days_since_moved_at_vs_move_type_operator_avg
    FROM enriched_warehouse
    WINDOW w_move_type_operator AS (PARTITION BY move_type, operator), w_warehouse_id_by_days_since_m AS (PARTITION BY warehouse_id ORDER BY days_since_moved_at DESC)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        sku_id,
        move_type,
        operator,
        unit_price,
        days_since_moved_at,
        unit_price_bucket,
        warehouse_wh_code,
        unit_price_bucket_sum_6_prior,
        distinct_warehouse_id_in_move_type_operator,
        distinct_sku_id_in_move_type_operator,
        rows_in_move_type_operator,
        unit_price_bucket_share_of_move_type_operator,
        ABS(unit_price_bucket - 300) AS unit_price_bucket_distance
    FROM with_trends
),
grouped AS (
    -- Group-level totals
    SELECT
        move_type,
        SUM(unit_price_bucket) AS total_unit_price_bucket,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids
    FROM annotated
    GROUP BY move_type
)
SELECT move_type
FROM grouped
ORDER BY move_type
LIMIT 20
