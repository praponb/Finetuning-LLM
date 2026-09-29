WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(amount, 2) OVER (PARTITION BY order_id ORDER BY amount, id) next_amount,
        NTILE(4) OVER (ORDER BY amount, id) amount_bucket,
        RANK() OVER (PARTITION BY method ORDER BY amount DESC NULLS FIRST) amount_rank,
        AVG(amount) OVER (PARTITION BY MONTH(paid_at), status ORDER BY amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) amount_avg_2,
        ROUND(amount / SUM(amount) OVER (PARTITION BY status), 4) amount_share
    FROM payments
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        next_amount,
        amount_bucket,
        amount_rank,
        YEAR(paid_at) paid_at_year
    FROM scored
    WHERE method IS DISTINCT FROM 'card'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        method,
        amount_bucket,
        amount_rank,
        COUNT(amount_bucket) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_count_amount_bucket,
        MAX(amount_bucket) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_max_amount_bucket,
        MIN(amount_bucket) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_min_amount_bucket,
        STDDEV_SAMP(next_amount) OVER (PARTITION BY MONTH(paid_at), order_id) paid_at_month_stddev_samp_next_amount
    FROM base
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        method,
        amount_bucket,
        amount_rank,
        last3_count_amount_bucket,
        last3_min_amount_bucket,
        paid_at_month_stddev_samp_next_amount
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY amount_bucket DESC NULLS FIRST) AS latest_rank
        FROM enriched
    ) x
    WHERE latest_rank = 1
),
grouped AS (
    -- Grouped metrics
    SELECT
        CASE WHEN last3_min_amount_bucket >= 8.9 THEN 'PREMIUM' ELSE 'STANDARD' END AS last3_min_amount_bucket_tier,
        method,
        CASE WHEN last3_count_amount_bucket >= 13 THEN 'LARGE' ELSE 'SMALL' END AS last3_count_amount_bucket_tier,
        APPROX_PERCENTILE(last3_count_amount_bucket, 0.95) AS last3_count_amount_bucket_p95,
        PERCENTILE_APPROX(amount_rank, 0.9) AS amount_rank_p90,
        COUNT(CASE WHEN method = 'card' THEN 1 END) AS card_count,
        COUNT(CASE WHEN method = 'paypal' THEN 1 END) AS paypal_count,
        PERCENTILE_APPROX(last3_min_amount_bucket, ARRAY(0.25, 0.5, 0.75)) AS last3_min_amount_bucket_quartiles,
        AVG(last3_min_amount_bucket) AS avg_last3_min_amount_bucket,
        RANK() OVER (PARTITION BY CASE WHEN last3_min_amount_bucket >= 8.9 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT(CASE WHEN method = 'paypal' THEN 1 END) DESC) AS paypal_count_rank
    FROM deduped
    GROUP BY CASE WHEN last3_min_amount_bucket >= 8.9 THEN 'PREMIUM' ELSE 'STANDARD' END, method, CASE WHEN last3_count_amount_bucket >= 13 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    last3_min_amount_bucket_tier,
    method,
    last3_count_amount_bucket_tier,
    last3_count_amount_bucket_p95,
    amount_rank_p90,
    paypal_count,
    last3_min_amount_bucket_quartiles,
    avg_last3_min_amount_bucket,
    paypal_count_rank
FROM grouped
ORDER BY method DESC
