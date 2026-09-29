WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(discount_pct * 0.9, 2) AS adjusted_discount_pct
    FROM promotions
    WHERE channel IS NOT NULL
      AND ends_on BETWEEN '2025-02-01' AND '2025-08-01'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        promo_code,
        adjusted_discount_pct,
        DENSE_RANK() OVER w_part AS discount_pct_dense_rank,
        SUM(adjusted_discount_pct) OVER (PARTITION BY DATE_TRUNC('YEAR', ends_on), promo_code) AS ends_on_period_sum_adjusted_discount_pct
    FROM cleaned
    WINDOW w_part AS (PARTITION BY promo_code ORDER BY discount_pct DESC)
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, ends_on_period_sum_adjusted_discount_pct + adjusted_discount_pct AS ends_on_period_sum_adjusted_discount_pct_plus_adjusted_discount_pct
    FROM with_windows
    WHERE channel IS DISTINCT FROM 'web'
),
base_promotions AS (
    -- Keep recent, valid rows
    SELECT
        *,
        discount_pct_dense_rank * adjusted_discount_pct AS discount_pct_dense_rank_times_adjusted_discount_pct,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM base
    WHERE channel IS NOT NULL
      AND ends_on_period_sum_adjusted_discount_pct BETWEEN 144 AND 266
),
with_windows_promotions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        adjusted_discount_pct,
        discount_pct_dense_rank,
        ends_on_period_sum_adjusted_discount_pct_plus_adjusted_discount_pct,
        discount_pct_dense_rank_times_adjusted_discount_pct,
        resolved_channel,
        SUM(discount_pct_dense_rank_times_adjusted_discount_pct) OVER w_part AS running_sum_discount_pct_dense_rank_times_adjusted_discount_pct,
        MIN(discount_pct_dense_rank_times_adjusted_discount_pct) OVER w_part AS running_min_discount_pct_dense_rank_times_adjusted_discount_pct,
        MAX(discount_pct_dense_rank_times_adjusted_discount_pct) OVER w_part AS running_max_discount_pct_dense_rank_times_adjusted_discount_pct
    FROM base_promotions
    WINDOW w_part AS (PARTITION BY promo_code ORDER BY adjusted_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(running_max_discount_pct_dense_rank_times_adjusted_discount_pct) OVER (ORDER BY running_sum_discount_pct_dense_rank_times_adjusted_discount_pct, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS running_max_discount_pct_dense_rank_times_adjusted_discount_pct_avg_6,
        NTILE(10) OVER (ORDER BY running_sum_discount_pct_dense_rank_times_adjusted_discount_pct, id) AS running_sum_discount_pct_dense_rank_times_adjusted_discount_pct_bucket,
        NTILE(3) OVER (ORDER BY adjusted_discount_pct, id) AS adjusted_discount_pct_bucket
    FROM with_windows_promotions
    WHERE discount_pct_dense_rank >= 4
)
-- Running and moving aggregates
SELECT
    id,
    adjusted_discount_pct,
    discount_pct_dense_rank,
    discount_pct_dense_rank_times_adjusted_discount_pct,
    running_min_discount_pct_dense_rank_times_adjusted_discount_pct,
    running_max_discount_pct_dense_rank_times_adjusted_discount_pct,
    adjusted_discount_pct_bucket,
    ROW_NUMBER() OVER (ORDER BY adjusted_discount_pct_bucket DESC, id) AS row_seq,
    SUM(discount_pct_dense_rank) OVER w_ordered AS cumulative_discount_pct_dense_rank,
    LEAD(adjusted_discount_pct_bucket, 1) OVER (ORDER BY running_sum_discount_pct_dense_rank_times_adjusted_discount_pct, id) AS next_adjusted_discount_pct_bucket,
    AVG(discount_pct_dense_rank_times_adjusted_discount_pct) OVER (ORDER BY ends_on_period_sum_adjusted_discount_pct_plus_adjusted_discount_pct, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS discount_pct_dense_rank_times_adjusted_discount_pct_avg_2,
    SUM(adjusted_discount_pct_bucket) OVER w_ordered AS running_adjusted_discount_pct_bucket
FROM metrics
WINDOW w_ordered AS (ORDER BY ends_on_period_sum_adjusted_discount_pct_plus_adjusted_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
