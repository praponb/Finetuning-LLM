WITH base_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        CONCAT_WS('-', status, product) AS status_product_label,
        DATE_TRUNC('WEEK', start_date) AS start_date_week
    FROM policies
),
without_matches AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM base_policys s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.status = 'EXPIRED'
              AND YEAR(p.end_date) = 2026
        )
      AND s.holder_id IS NOT NULL
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        end_date,
        premium_bucket,
        start_date_week,
        LAST_VALUE(start_date_week) OVER (ORDER BY start_date_week DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date_week,
        premium_bucket - LAG(premium_bucket) OVER w_all_by_start_date_week AS premium_bucket_change,
        ROUND(premium_bucket - AVG(premium_bucket) OVER (PARTITION BY MONTH(start_date_week), YEAR(end_date)), 2) AS premium_bucket_vs_start_date_week_month_end_date
    FROM without_matches
    WINDOW w_all_by_start_date_week AS (ORDER BY start_date_week DESC, id)
)
-- Subtotals with GROUPING SETS
SELECT
    holder_id,
    product,
    ROUND(AVG(premium_bucket_change), 2) AS avg_premium_bucket_change,
    COUNT(DISTINCT holder_id) AS distinct_holder_ids,
    COUNT(DISTINCT id) AS distinct_ids
FROM windowed
GROUP BY GROUPING SETS ((holder_id, product), (holder_id), (product))
