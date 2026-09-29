WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        MONTH(end_date) AS end_date_month,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM policies
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, COUNT(*) AS claim_count
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM filtered s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), product ORDER BY premium, id) AS premium_quintile,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY end_date_month)) AS distinct_status_in_end_date_month
    FROM summarized_rows
),
status_stats AS (
    -- Max premium_bucket per status
    SELECT status, MAX(premium_bucket) AS max_premium_bucket
    FROM sequenced
    GROUP BY status
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_premium_bucket,
        ROUND(s.premium_bucket - g.max_premium_bucket, 2) AS premium_bucket_minus_max
    FROM sequenced s
    JOIN status_stats g ON g.status = s.status
    WHERE s.premium_bucket = g.max_premium_bucket
)
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    end_date,
    status,
    end_date_month,
    premium_bucket,
    distinct_status_in_end_date_month,
    max_premium_bucket,
    ROUND(premium_bucket_minus_max * 1.07, 2) AS premium_bucket_minus_max_adj,
    ROUND(premium_quintile / NULLIF(premium_bucket_minus_max, 0), 3) AS premium_quintile_per_premium_bucket_minus_max
FROM above_average
ORDER BY end_date DESC, max_premium_bucket
