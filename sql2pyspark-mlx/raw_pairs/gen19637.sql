WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(amount) OVER (ORDER BY paid_at, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS amount_avg_5,
        NTILE(3) OVER (PARTITION BY status ORDER BY amount DESC, id) AS amount_bucket
    FROM payments
    WHERE status NOT IN ('SUCCESS', 'FAILED')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        amount,
        paid_at,
        status,
        amount_avg_5,
        MAX(amount) OVER w_grp AS amount_max_90d,
        NTILE(5) OVER (PARTITION BY order_id, status ORDER BY amount, id) AS amount_bucket_2,
        ROW_NUMBER() OVER w_grp_2 AS event_seq,
        FIRST_VALUE(amount_bucket) IGNORE NULLS OVER (PARTITION BY status ORDER BY amount_avg_5, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount_bucket
    FROM metrics
    WINDOW w_grp AS (PARTITION BY status, method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW), w_grp_2 AS (PARTITION BY order_id ORDER BY amount_avg_5 DESC, id)
),
metrics_payments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        amount,
        paid_at,
        status,
        amount_avg_5,
        first_known_amount_bucket,
        MAX(first_known_amount_bucket) OVER (PARTITION BY status ORDER BY amount_max_90d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_first_known_amount_bucket,
        SUM(first_known_amount_bucket) OVER (PARTITION BY status ORDER BY amount_max_90d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_first_known_amount_bucket,
        MIN(first_known_amount_bucket) OVER (PARTITION BY status ORDER BY amount_max_90d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_first_known_amount_bucket
    FROM with_windows
),
with_windows_payments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(amount_avg_5) OVER (PARTITION BY status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_avg_5_sum_90d,
        ROUND(running_sum_first_known_amount_bucket / SUM(running_sum_first_known_amount_bucket) OVER (PARTITION BY status), 4) AS running_sum_first_known_amount_bucket_share,
        STDDEV_SAMP(running_max_first_known_amount_bucket) OVER (PARTITION BY YEAR(paid_at), status) AS paid_at_year_stddev_samp_running_max_first_known_amount_bucket
    FROM metrics_payments
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(running_sum_first_known_amount_bucket AS INT) AS running_sum_first_known_amount_bucket_int
    FROM with_windows_payments
    WHERE running_sum_first_known_amount_bucket BETWEEN 18 AND 33
)
-- Multi-level aggregation
SELECT
    status,
    CASE WHEN running_max_first_known_amount_bucket >= 8.6 THEN 'LARGE' ELSE 'SMALL' END AS running_max_first_known_amount_bucket_tier,
    COUNT_IF(status = 'FAILED') AS failed_events,
    ROUND(AVG(running_sum_first_known_amount_bucket_share), 2) AS avg_running_sum_first_known_amount_bucket_share,
    MAX(paid_at) AS last_paid_at,
    COUNT(DISTINCT id) AS distinct_ids,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS span_minutes,
    RANK() OVER (PARTITION BY status ORDER BY ROUND(AVG(running_sum_first_known_amount_bucket_share), 2) DESC) AS avg_running_sum_first_known_amount_bucket_share_rank
FROM base
GROUP BY status, running_max_first_known_amount_bucket_tier
HAVING ROUND(AVG(running_sum_first_known_amount_bucket_share), 2) > 0;
