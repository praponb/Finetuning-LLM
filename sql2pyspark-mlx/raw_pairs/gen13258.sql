WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN merchant = 'Amazon' THEN 1 ELSE 0 END AS is_amazon
    FROM transactions
    WHERE merchant IS DISTINCT FROM 'Grab'
      AND account_id IN (2, 13, 40)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        merchant,
        is_flagged,
        SUM(is_amazon) OVER (ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_is_amazon,
        NTILE(3) OVER (ORDER BY is_amazon ASC, id ASC) AS is_amazon_bucket,
        ROUND(amount / SUM(amount) OVER (PARTITION BY merchant), 4) AS amount_share
    FROM cleaned
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        running_is_amazon,
        is_amazon_bucket,
        SUM(is_amazon_bucket) OVER (PARTITION BY account_id ORDER BY running_is_amazon ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_is_amazon_bucket,
        AVG(is_amazon_bucket) OVER (PARTITION BY account_id ORDER BY running_is_amazon ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_is_amazon_bucket,
        MAX(is_amazon_bucket) OVER (PARTITION BY account_id ORDER BY running_is_amazon ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_is_amazon_bucket,
        LEAD(running_is_amazon, 1) OVER (PARTITION BY merchant, account_id ORDER BY is_amazon_bucket ASC, id ASC) AS next_running_is_amazon
    FROM enriched
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        running_is_amazon,
        is_amazon_bucket,
        running_sum_is_amazon_bucket,
        running_avg_is_amazon_bucket,
        running_max_is_amazon_bucket,
        next_running_is_amazon,
        ROUND(running_avg_is_amazon_bucket, 2) AS running_avg_is_amazon_bucket_rounded
    FROM metrics
    WHERE running_avg_is_amazon_bucket > 2.9
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        is_amazon_bucket,
        running_sum_is_amazon_bucket,
        running_max_is_amazon_bucket,
        AVG(next_running_is_amazon) OVER (ORDER BY is_amazon_bucket ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS next_running_is_amazon_avg_2,
        LAST_VALUE(running_avg_is_amazon_bucket_rounded, TRUE) OVER (ORDER BY running_avg_is_amazon_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_avg_is_amazon_bucket_rounded
    FROM filtered
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(running_max_is_amazon_bucket * 1.1, 2) AS running_max_is_amazon_bucket_adjusted
    FROM scored
    WHERE next_running_is_amazon_avg_2 > 112
),
cleaned_transactions AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        running_sum_is_amazon_bucket + last_known_running_avg_is_amazon_bucket_rounded AS running_sum_is_amazon_bucket_plus_last_known_running_avg_is_amazon_bucket_rounded,
        CEIL(next_running_is_amazon_avg_2) AS next_running_is_amazon_avg_2_whole
    FROM base
    WHERE running_sum_is_amazon_bucket BETWEEN 32 AND 37
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN next_running_is_amazon_avg_2_whole >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_running_is_amazon_avg_2_whole_tier,
    COUNT(*) AS row_count,
    ROUND(AVG(last_known_running_avg_is_amazon_bucket_rounded), 2) AS avg_last_known_running_avg_is_amazon_bucket_rounded
FROM cleaned_transactions
GROUP BY 1
HAVING COUNT(*) >= 1
ORDER BY avg_last_known_running_avg_is_amazon_bucket_rounded DESC;
