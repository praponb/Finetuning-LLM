WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        status,
        CASE WHEN monthly_fee >= 234 THEN 'High' WHEN monthly_fee >= 49 THEN 'Medium' ELSE 'Low' END AS monthly_fee_band
    FROM subscriptions
    WHERE customer_id IS NOT NULL
      AND start_date >= DATE '2023-09-05'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        start_date,
        end_date,
        monthly_fee,
        LAST_VALUE(monthly_fee) IGNORE NULLS OVER (ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY customer_id ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee,
        ROW_NUMBER() OVER (ORDER BY monthly_fee DESC, id) AS event_seq
    FROM base
),
scored_subscriptions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(event_seq, 2) OVER w_hist AS next_event_seq,
        NTILE(4) OVER (ORDER BY running_monthly_fee, id) AS running_monthly_fee_bucket,
        SUM(last_known_monthly_fee) OVER w_hist_2 AS cumulative_last_known_monthly_fee,
        NTILE(10) OVER (ORDER BY running_monthly_fee DESC, id) AS running_monthly_fee_bucket_2
    FROM scored
    WINDOW w_hist AS (ORDER BY start_date, id), w_hist_2 AS (ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(monthly_fee) OVER w_running AS running_count_monthly_fee,
        SUM(monthly_fee) OVER w_running AS running_sum_monthly_fee,
        MAX(monthly_fee) OVER w_running AS running_max_monthly_fee,
        ROW_NUMBER() OVER (ORDER BY next_event_seq DESC, id) AS seq_no
    FROM scored_subscriptions
    WINDOW w_running AS (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    start_date,
    end_date,
    monthly_fee,
    last_known_monthly_fee,
    running_monthly_fee,
    event_seq,
    next_event_seq,
    running_monthly_fee_bucket
FROM with_windows;
