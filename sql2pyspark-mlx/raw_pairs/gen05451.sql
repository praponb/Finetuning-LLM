WITH joined AS (
    -- Attach customers attributes
    SELECT a.*, p.email
    FROM subscriptions a
    LEFT JOIN customers p
        ON a.customer_id <=> p.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        start_date,
        end_date,
        monthly_fee,
        MAX(monthly_fee) OVER (PARTITION BY plan ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_monthly_fee,
        COUNT(monthly_fee) OVER (PARTITION BY plan ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY plan ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_monthly_fee
    FROM joined
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        start_date,
        end_date,
        DATEDIFF(end_date, start_date) / 7 AS weeks_start_date_to_end_date
    FROM with_windows
    WHERE end_date > DATE '2023-06-20'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (ORDER BY weeks_start_date_to_end_date DESC, id) AS weeks_start_date_to_end_date_bucket,
        LAST_VALUE(weeks_start_date_to_end_date) IGNORE NULLS OVER (ORDER BY weeks_start_date_to_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_weeks_start_date_to_end_date
    FROM filtered
),
filtered_subscriptions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(weeks_start_date_to_end_date, 1) AS weeks_start_date_to_end_date_rounded,
        CEIL(weeks_start_date_to_end_date) AS weeks_start_date_to_end_date_whole
    FROM metrics
    WHERE start_date > TO_DATE('2023-09-11')
      AND last_known_weeks_start_date_to_end_date > 44
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', start_date) AS activity_month,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(weeks_start_date_to_end_date, 0.95) AS weeks_start_date_to_end_date_p95,
    APPROX_PERCENTILE(weeks_start_date_to_end_date_rounded, ARRAY(0.25, 0.5, 0.75)) AS weeks_start_date_to_end_date_rounded_quartiles,
    PERCENTILE_APPROX(weeks_start_date_to_end_date_whole, 0.9) AS weeks_start_date_to_end_date_whole_p90
FROM filtered_subscriptions
GROUP BY DATE_TRUNC('MONTH', start_date)
HAVING num_rows < 1000
ORDER BY activity_month DESC;
