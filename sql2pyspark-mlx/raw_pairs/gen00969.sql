WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS('-', plan, status) plan_status_key
    FROM subscriptions
    WHERE end_date > DATE '2023-05-10'
      AND monthly_fee > 150
),
filtered_subscriptions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        plan_status_key,
        ROUND(monthly_fee * 100, 2) adjusted_monthly_fee
    FROM filtered
    WHERE customer_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_hist event_seq,
        SUM(monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_monthly_fee,
        NTILE(5) OVER (PARTITION BY plan ORDER BY adjusted_monthly_fee, id) adjusted_monthly_fee_bucket
    FROM filtered_subscriptions
    WINDOW w_hist AS (ORDER BY end_date, id)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        start_date,
        plan_status_key,
        adjusted_monthly_fee,
        event_seq,
        adjusted_monthly_fee_bucket,
        AVG(adjusted_monthly_fee) OVER (PARTITION BY plan ORDER BY running_monthly_fee, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) adjusted_monthly_fee_avg_6,
        LAG(monthly_fee, 1, 0) OVER (PARTITION BY MONTH(end_date), customer_id ORDER BY adjusted_monthly_fee, id) prev_monthly_fee
    FROM with_windows
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.start_date,
        s.adjusted_monthly_fee,
        s.event_seq,
        c.age
    FROM metrics s
    LEFT JOIN customers c
        ON s.customer_id <=> c.id
),
metrics_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(age / SUM(age) OVER (PARTITION BY customer_id), 4) age_share,
        LAG(age, 1) OVER w_hist prev_age,
        NTILE(3) OVER (PARTITION BY customer_id ORDER BY event_seq DESC, id) event_seq_bucket,
        RANK() OVER w_hist_2 adjusted_monthly_fee_rank
    FROM with_dims
    WHERE event_seq <= 3
    WINDOW w_hist AS (ORDER BY start_date, id), w_hist_2 AS (PARTITION BY customer_id ORDER BY adjusted_monthly_fee DESC)
),
daily AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('QUARTER', start_date) AS quarter_start,
        APPROX_PERCENTILE(event_seq_bucket, 0.95) AS event_seq_bucket_p95,
        COUNT(*) AS num_rows,
        COUNT(*) AS row_count
    FROM metrics_subscriptions
    GROUP BY 1
    HAVING APPROX_PERCENTILE(event_seq_bucket, 0.95) >= 0
)
SELECT
    quarter_start,
    event_seq_bucket_p95,
    num_rows
FROM daily
ORDER BY event_seq_bucket_p95 DESC NULLS LAST;
