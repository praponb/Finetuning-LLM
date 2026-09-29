WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('/', plan, status) AS plan_status_key,
        CEIL(monthly_fee) AS monthly_fee_whole
    FROM subscriptions
    WHERE end_date > TO_DATE('2023-08-03')
      AND customer_id IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        end_date,
        monthly_fee,
        status,
        plan_status_key,
        monthly_fee_whole,
        AVG(monthly_fee) OVER (PARTITION BY YEAR(start_date), status ORDER BY start_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_6,
        PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', start_date), status ORDER BY monthly_fee_whole DESC) AS monthly_fee_whole_percent_rank,
        DENSE_RANK() OVER (PARTITION BY YEAR(start_date), status ORDER BY monthly_fee) AS monthly_fee_dense_rank
    FROM cleaned
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        end_date,
        monthly_fee,
        status,
        plan_status_key,
        monthly_fee_whole,
        monthly_fee_avg_6,
        monthly_fee_dense_rank,
        CASE WHEN plan IN ('enterprise', 'premium') THEN 'Priority' ELSE 'Other' END AS plan_group
    FROM with_windows
    WHERE monthly_fee > 230
),
cleaned_subscriptions AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        monthly_fee,
        monthly_fee_whole,
        monthly_fee_avg_6,
        monthly_fee_dense_rank,
        plan_group,
        ROUND(monthly_fee_avg_6, 0) AS monthly_fee_avg_6_rounded,
        monthly_fee_whole + monthly_fee AS monthly_fee_whole_plus_monthly_fee
    FROM base
    WHERE plan IN ('enterprise', 'basic')
)
SELECT
    id,
    plan,
    monthly_fee,
    monthly_fee_avg_6,
    monthly_fee_dense_rank,
    monthly_fee_avg_6_rounded
FROM cleaned_subscriptions
