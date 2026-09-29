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
        unit_price
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        FLOOR(qty / 100) * 100 AS qty_bucket
    FROM filtered_moves
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        move_type,
        unit_price,
        qty_bucket,
        ROUND(unit_price / NULLIF(qty_bucket, 0), 3) AS unit_price_per_qty_bucket
    FROM enriched
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(unit_price_per_qty_bucket) IGNORE NULLS OVER (PARTITION BY move_type ORDER BY unit_price_per_qty_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price_per_qty_bucket,
        ROUND(qty_bucket - AVG(qty_bucket) OVER (PARTITION BY move_type), 2) AS qty_bucket_vs_move_type_avg,
        LAG(unit_price_per_qty_bucket, 2, 0) OVER (PARTITION BY move_type ORDER BY unit_price_per_qty_bucket DESC, id) AS prev2_unit_price_per_qty_bucket,
        LAST_VALUE(unit_price) OVER (PARTITION BY move_type ORDER BY unit_price_per_qty_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price
    FROM scored
),
move_type_stats AS (
    -- Median first_known_unit_price_per_qty_bucket per move_type
    SELECT move_type, PERCENTILE_APPROX(first_known_unit_price_per_qty_bucket, 0.5) AS median_first_known_unit_price_per_qty_bucket
    FROM with_trends
    GROUP BY move_type
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_first_known_unit_price_per_qty_bucket,
        ROUND(s.first_known_unit_price_per_qty_bucket - g.median_first_known_unit_price_per_qty_bucket, 2) AS first_known_unit_price_per_qty_bucket_minus_medi
    FROM with_trends s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.first_known_unit_price_per_qty_bucket >= g.median_first_known_unit_price_per_qty_bucket
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM vs_group
    WHERE unit_price_per_qty_bucket > (SELECT MIN(unit_price_per_qty_bucket) FROM vs_group)
),
grouped AS (
    -- Group-level totals
    SELECT
        move_type,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        ROUND(AVG(last_unit_price), 2) AS avg_last_unit_price,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_activity
    GROUP BY move_type
    HAVING SUM(unit_price) > 111.7
)
-- Final projection
SELECT
    move_type,
    move_type_list,
    avg_last_unit_price,
    distinct_ids,
    LENGTH(TRIM(move_type)) AS move_type_len,
    COALESCE(avg_last_unit_price, 0.0) AS avg_last_unit_price_filled
FROM grouped
WHERE avg_last_unit_price >= 100.8
ORDER BY move_type_len DESC, move_type
LIMIT 20
