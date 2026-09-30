WITH recent AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM policies
    WHERE product IN ('HOME', 'LIFE')
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(premium) OVER (PARTITION BY holder_id) AS holder_id_sum_premium,
        SUM(premium_bucket) OVER (ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium_bucket,
        RANK() OVER w_product_status_by_premium AS premium_rank,
        MAX(premium_bucket) OVER (ORDER BY premium, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS premium_bucket_max_6_prior,
        MAX(premium_bucket) OVER (PARTITION BY holder_id) AS holder_id_max_premium_bucket,
        MIN(premium) OVER (PARTITION BY holder_id) AS holder_id_min_premium
    FROM recent
    WINDOW w_product_status_by_premium AS (PARTITION BY product, status ORDER BY premium DESC, id)
),
status_or_unknown_stats AS (
    -- Avg running_premium_bucket per status_or_unknown
    SELECT status_or_unknown, AVG(running_premium_bucket) AS avg_running_premium_bucket
    FROM sequenced
    GROUP BY status_or_unknown
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_running_premium_bucket,
        ROUND(s.running_premium_bucket - g.avg_running_premium_bucket, 2) AS running_premium_bucket_minus_avg
    FROM sequenced s
    JOIN status_or_unknown_stats g ON g.status_or_unknown = s.status_or_unknown
    WHERE s.running_premium_bucket > g.avg_running_premium_bucket
),
joined_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.premium_bucket,
        s.days_start_date_to_end_date,
        s.holder_id_sum_premium,
        s.running_premium_bucket,
        s.premium_rank,
        s.premium_bucket_max_6_prior,
        s.holder_id_max_premium_bucket,
        s.holder_id_min_premium,
        s.avg_running_premium_bucket,
        s.running_premium_bucket_minus_avg,
        p.segment AS holder_segment
    FROM above_average s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
-- Subtotals with GROUPING SETS
SELECT
    holder_id,
    product,
    GROUPING(holder_id) AS is_total_holder_id,
    SUM(CASE WHEN holder_segment = 'VIP' THEN premium_bucket ELSE 0 END) AS premium_bucket_holder_segment_vip,
    SUM(premium_rank) AS total_premium_rank,
    COUNT(DISTINCT holder_id) AS distinct_holder_ids
FROM joined_holder
GROUP BY GROUPING SETS ((holder_id, product), (holder_id))
ORDER BY holder_id NULLS LAST, product NULLS LAST
