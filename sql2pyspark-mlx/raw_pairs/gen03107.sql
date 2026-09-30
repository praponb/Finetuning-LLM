WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        start_date,
        end_date,
        monthly_fee,
        status,
        LAST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY MONTH(start_date) ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee,
        MAX(monthly_fee) OVER w_ordered AS end_date_period_max_monthly_fee
    FROM subscriptions
    WINDOW w_ordered AS (PARTITION BY DATE_TRUNC('WEEK', end_date), customer_id)
),
metrics_subscriptions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        start_date,
        end_date,
        monthly_fee,
        status,
        last_known_monthly_fee,
        cumulative_monthly_fee,
        SUM(end_date_period_max_monthly_fee) OVER (PARTITION BY status ORDER BY last_known_monthly_fee, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_end_date_period_max_monthly_fee,
        AVG(end_date_period_max_monthly_fee) OVER (PARTITION BY status ORDER BY last_known_monthly_fee, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_end_date_period_max_monthly_fee,
        COUNT(end_date_period_max_monthly_fee) OVER (PARTITION BY status ORDER BY last_known_monthly_fee, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_end_date_period_max_monthly_fee
    FROM metrics
),
metrics_subscriptions_2 AS (
    -- Ranking and gap features
    SELECT
        id,
        start_date,
        monthly_fee,
        last_known_monthly_fee,
        cumulative_monthly_fee,
        last3_count_end_date_period_max_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY YEAR(end_date), status ORDER BY last3_sum_end_date_period_max_monthly_fee, id) AS row_seq,
        AVG(last3_avg_end_date_period_max_monthly_fee) OVER (ORDER BY last3_avg_end_date_period_max_monthly_fee, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS last3_avg_end_date_period_max_monthly_fee_avg_5_prior
    FROM metrics_subscriptions
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY DATE_TRUNC('WEEK', start_date)), 4) AS monthly_fee_share,
        LAG(monthly_fee, 2) OVER (PARTITION BY DATE_TRUNC('WEEK', start_date) ORDER BY last3_avg_end_date_period_max_monthly_fee_avg_5_prior, id) AS prev_monthly_fee,
        NTILE(5) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY cumulative_monthly_fee, id) AS cumulative_monthly_fee_bucket
    FROM metrics_subscriptions_2
    WHERE cumulative_monthly_fee > 831
)
SELECT
    id,
    start_date,
    monthly_fee,
    last_known_monthly_fee,
    last3_count_end_date_period_max_monthly_fee,
    row_seq,
    monthly_fee_share,
    prev_monthly_fee,
    cumulative_monthly_fee_bucket
FROM with_windows
ORDER BY id, monthly_fee_share DESC
LIMIT 10;
