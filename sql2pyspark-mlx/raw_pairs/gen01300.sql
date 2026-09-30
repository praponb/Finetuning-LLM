WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        end_date,
        status,
        AVG(monthly_fee) OVER w_recent AS running_avg_monthly_fee,
        MAX(monthly_fee) OVER w_recent AS running_max_monthly_fee,
        MIN(monthly_fee) OVER w_recent AS running_min_monthly_fee,
        COUNT(monthly_fee) OVER w_recent AS running_count_monthly_fee
    FROM subscriptions
    WINDOW w_recent AS (PARTITION BY plan ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        running_avg_monthly_fee,
        running_min_monthly_fee,
        running_count_monthly_fee,
        LEAST(running_count_monthly_fee, running_avg_monthly_fee) AS least_running_count_monthly_fee_running_avg_monthly_fee,
        ROUND(running_max_monthly_fee, 0) AS running_max_monthly_fee_rounded
    FROM scored
    WHERE end_date >= DATE '2023-08-04'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        running_avg_monthly_fee,
        running_min_monthly_fee,
        running_count_monthly_fee,
        least_running_count_monthly_fee_running_avg_monthly_fee,
        running_max_monthly_fee_rounded,
        CEIL(running_max_monthly_fee_rounded) AS running_max_monthly_fee_rounded_whole
    FROM valid
    WHERE least_running_count_monthly_fee_running_avg_monthly_fee <= 42
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        running_avg_monthly_fee,
        least_running_count_monthly_fee_running_avg_monthly_fee,
        SUM(running_min_monthly_fee) OVER (ORDER BY running_max_monthly_fee_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_min_monthly_fee,
        CUME_DIST() OVER (ORDER BY running_min_monthly_fee DESC NULLS FIRST) AS running_min_monthly_fee_cume_dist
    FROM filtered
),
scored_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        id,
        running_avg_monthly_fee,
        running_running_min_monthly_fee,
        FIRST_VALUE(running_avg_monthly_fee, TRUE) OVER (ORDER BY running_min_monthly_fee_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_avg_monthly_fee,
        LAST_VALUE(running_avg_monthly_fee) IGNORE NULLS OVER w_hist AS last_known_running_avg_monthly_fee,
        ROW_NUMBER() OVER (ORDER BY running_running_min_monthly_fee, id) AS row_seq
    FROM enriched
    WINDOW w_hist AS (ORDER BY running_avg_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered_subscriptions AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(running_avg_monthly_fee AS INT) AS running_avg_monthly_fee_int
    FROM scored_subscriptions
    WHERE running_avg_monthly_fee < 110
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY running_avg_monthly_fee DESC, id) AS row_seq_2,
        NTILE(10) OVER (ORDER BY running_avg_monthly_fee_int DESC, id) AS running_avg_monthly_fee_int_bucket
    FROM filtered_subscriptions
)
SELECT
    id,
    running_avg_monthly_fee,
    last_known_running_avg_monthly_fee,
    row_seq,
    running_avg_monthly_fee_int,
    row_seq_2,
    running_avg_monthly_fee_int_bucket
FROM metrics
ORDER BY running_avg_monthly_fee_int DESC
