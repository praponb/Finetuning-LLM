WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        LAG(amount, 1) OVER (PARTITION BY merchant, txn_type ORDER BY amount, id) AS prev_amount,
        MAX(amount) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_max_7d
    FROM transactions
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        merchant,
        is_flagged,
        prev_amount,
        NTILE(5) OVER (ORDER BY amount, id) AS amount_bucket,
        AVG(amount_max_7d) OVER (PARTITION BY merchant ORDER BY amount_max_7d, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS amount_max_7d_avg_6_prior
    FROM with_windows
    WHERE amount BETWEEN 1500 AND 3000
),
with_windows_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(amount_bucket) OVER w_hist AS merchant_stddev_samp_amount_bucket,
        MIN(amount_bucket) OVER w_hist AS merchant_min_amount_bucket
    FROM enriched
    WINDOW w_hist AS (PARTITION BY merchant)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        merchant,
        is_flagged,
        prev_amount,
        amount_bucket,
        amount_max_7d_avg_6_prior,
        merchant_stddev_samp_amount_bucket,
        ROUND(merchant_stddev_samp_amount_bucket / amount_bucket, 2) AS merchant_stddev_samp_amount_bucket_per_amount_bucket
    FROM with_windows_transactions
    WHERE merchant IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(amount_bucket) OVER w_part AS last7_max_amount_bucket,
        AVG(amount_bucket) OVER w_part AS last7_avg_amount_bucket,
        SUM(amount_bucket) OVER w_part AS last7_sum_amount_bucket
    FROM filtered
    WINDOW w_part AS (PARTITION BY merchant ORDER BY amount_bucket, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    merchant,
    ROUND(AVG(last7_avg_amount_bucket), 2) AS avg_last7_avg_amount_bucket,
    SUM(last7_avg_amount_bucket) AS total_last7_avg_amount_bucket,
    COUNT(*) FILTER (WHERE merchant = '7-Eleven') AS 7_eleven_rows,
    PERCENTILE_APPROX(merchant_stddev_samp_amount_bucket, 0.95) AS merchant_stddev_samp_amount_bucket_p95,
    SIZE(COLLECT_SET(merchant)) AS num_merchants,
    PERCENTILE_APPROX(merchant_stddev_samp_amount_bucket, 0.95) AS merchant_stddev_samp_amount_bucket_p95_2
FROM metrics
GROUP BY 1
ORDER BY merchant_stddev_samp_amount_bucket_p95_2 NULLS FIRST;
