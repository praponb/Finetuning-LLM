WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(monthly_fee, 1) AS monthly_fee_rounded
    FROM subscriptions
    WHERE end_date >= DATE '2023-04-25'
      AND customer_id IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY monthly_fee DESC, id) AS dedup_rank
        FROM filtered
    ) x
    WHERE dedup_rank = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, DATE_TRUNC('YEAR', end_date) AS end_date_period
    FROM latest
    WHERE end_date > TO_DATE('2023-10-08')
),
filtered_subscriptions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee_rounded,
        end_date_period,
        DATE_TRUNC('year', start_date) AS start_date_period,
        dedup_rank - monthly_fee_rounded AS dedup_rank_minus_monthly_fee_rounded
    FROM cleaned
    WHERE customer_id IS NULL
      AND end_date >= TO_DATE('2023-05-23')
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', end_date_period) AS year_start,
    ROUND(AVG(dedup_rank_minus_monthly_fee_rounded), 2) AS avg_dedup_rank_minus_monthly_fee_rounded,
    ROUND(SUM(monthly_fee_rounded) / NULLIF(COUNT(CASE WHEN plan = 'premium' THEN 1 END), 0), 2) AS monthly_fee_rounded_per_premium,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    COUNT_IF(plan = 'enterprise') AS enterprise_events,
    SIZE(COLLECT_SET(plan)) AS num_plans_2
FROM filtered_subscriptions
GROUP BY 1
