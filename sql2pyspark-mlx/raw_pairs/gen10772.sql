WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        LOWER(plan) AS plan_lower,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(days_start_date_to_end_date) OVER (PARTITION BY customer_id ORDER BY days_start_date_to_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_days_start_date_to_end_date,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY days_start_date_to_end_date DESC) AS days_start_date_to_end_date_percent_rank
    FROM cleaned
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        plan_lower,
        days_start_date_to_end_date_percent_rank
    FROM with_windows
    QUALIFY RANK() OVER (PARTITION BY plan ORDER BY cumulative_days_start_date_to_end_date DESC NULLS LAST) = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        plan_lower,
        COALESCE(days_start_date_to_end_date_percent_rank, 0) AS days_start_date_to_end_date_percent_rank_filled,
        UPPER(plan) AS plan_upper
    FROM latest
    WHERE start_date > DATE '2023-03-13'
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', start_date) AS year_start,
    plan,
    STDDEV_SAMP(monthly_fee) AS monthly_fee_stddev,
    COUNT(*) AS cnt,
    COUNT(DISTINCT plan) AS distinct_plans,
    COUNT(*) AS event_count
FROM base
GROUP BY 1, 2
ORDER BY event_count NULLS FIRST;
