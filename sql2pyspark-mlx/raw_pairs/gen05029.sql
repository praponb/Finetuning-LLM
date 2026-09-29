WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee,
        status,
        CAST(monthly_fee AS INT) AS monthly_fee_int,
        SUBSTRING(plan, 1, 2) AS plan_prefix
    FROM subscriptions
    WHERE end_date >= DATE '2023-04-28'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(monthly_fee_int) OVER (PARTITION BY MONTH(start_date), plan) AS start_date_month_avg_monthly_fee_int,
        AVG(monthly_fee) OVER (PARTITION BY DATE_TRUNC('WEEK', start_date), status ORDER BY monthly_fee ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_6,
        LAG(monthly_fee_int, 1) OVER (PARTITION BY MONTH(start_date), customer_id ORDER BY start_date ASC, id ASC) AS prev_monthly_fee_int
    FROM valid
    WHERE customer_id IS NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date_month_avg_monthly_fee_int DESC NULLS LAST, id ASC) AS latest_rank
        FROM scored
    ) t
    WHERE latest_rank = 1
),
valid_subscriptions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        status,
        monthly_fee_int,
        plan_prefix,
        monthly_fee_avg_6,
        prev_monthly_fee_int,
        latest_rank,
        ROUND(monthly_fee_int, 1) AS monthly_fee_int_rounded
    FROM ranked
    WHERE start_date >= DATE '2023-03-21'
      AND customer_id IN (8, 15)
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', start_date) AS week_start,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    COUNT(CASE WHEN plan = 'premium' THEN 1 END) AS premium_count,
    COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count,
    COUNT_IF(plan = 'basic') AS basic_events,
    COUNT(*) FILTER (WHERE status = 'CANCELLED') AS cancelled_rows
FROM valid_subscriptions
GROUP BY DATE_TRUNC('WEEK', start_date)
HAVING COUNT_IF(plan = 'basic') >= 1
ORDER BY distinct_customer_ids DESC NULLS LAST;
