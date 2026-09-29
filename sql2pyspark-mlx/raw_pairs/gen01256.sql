WITH with_windows AS (
    SELECT
        id,
        plan,
        status,
        LEAD(monthly_fee) OVER w_hist AS next_monthly_fee,
        LAG(monthly_fee) OVER w_hist AS prev_monthly_fee,
        ROW_NUMBER() OVER w_hist AS row_seq,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id, status), 4) AS monthly_fee_share
    FROM subscriptions
    WINDOW w_hist AS (PARTITION BY status ORDER BY start_date, id)
),
scored AS (
    SELECT
        id,
        next_monthly_fee,
        row_seq,
        ROUND(next_monthly_fee / SUM(next_monthly_fee) OVER (PARTITION BY status, plan), 4) AS next_monthly_fee_share,
        NTILE(4) OVER (PARTITION BY plan ORDER BY monthly_fee_share DESC, id) AS monthly_fee_share_bucket
    FROM with_windows
),
scored_subscriptions AS (
    SELECT
        *,
        RANK() OVER w_hist AS row_seq_rank,
        AVG(row_seq) OVER (ORDER BY row_seq, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS row_seq_avg_3_prior,
        LEAD(next_monthly_fee_share, 2) OVER (ORDER BY monthly_fee_share_bucket, id) AS next_next_monthly_fee_share
    FROM scored
    WHERE monthly_fee_share_bucket <= 4
    WINDOW w_hist AS (ORDER BY row_seq DESC)
),
valid AS (
    SELECT
        *,
        row_seq_avg_3_prior - next_monthly_fee_share AS row_seq_avg_3_prior_minus_next_monthly_fee_share,
        ROUND(next_monthly_fee_share, 0) AS next_monthly_fee_share_rounded
    FROM scored_subscriptions
    WHERE next_next_monthly_fee_share <= 0.6
),
scored_subscriptions_2 AS (
    SELECT
        id,
        row_seq,
        next_monthly_fee_share,
        row_seq_rank,
        row_seq_avg_3_prior,
        next_next_monthly_fee_share,
        row_seq_avg_3_prior_minus_next_monthly_fee_share,
        next_monthly_fee_share_rounded,
        MAX(row_seq_avg_3_prior) OVER (ORDER BY next_monthly_fee_share_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_row_seq_avg_3_prior,
        COUNT(row_seq_avg_3_prior) OVER (ORDER BY next_monthly_fee_share_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_row_seq_avg_3_prior,
        AVG(row_seq_avg_3_prior) OVER (ORDER BY next_monthly_fee_share_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_row_seq_avg_3_prior,
        MIN(row_seq_avg_3_prior) OVER (ORDER BY next_monthly_fee_share_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_row_seq_avg_3_prior,
        AVG(next_monthly_fee) OVER (ORDER BY row_seq_avg_3_prior_minus_next_monthly_fee_share, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS next_monthly_fee_avg_2
    FROM valid
)
SELECT
    CASE WHEN row_seq_avg_3_prior_minus_next_monthly_fee_share >= 72 THEN 'HIGH' ELSE 'LOW' END AS row_seq_avg_3_prior_minus_next_monthly_fee_share_tier,
    CASE WHEN running_max_row_seq_avg_3_prior >= 6.8 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_max_row_seq_avg_3_prior_tier,
    PERCENTILE_APPROX(next_monthly_fee_share_rounded, ARRAY(0.25, 0.5, 0.75)) AS next_monthly_fee_share_rounded_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(row_seq_avg_3_prior_minus_next_monthly_fee_share, 0.95) AS row_seq_avg_3_prior_minus_next_monthly_fee_share_p95,
    COUNT(*) AS event_count
FROM scored_subscriptions_2
GROUP BY 1, 2
