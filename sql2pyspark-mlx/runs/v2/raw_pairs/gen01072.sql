WITH base AS (
    SELECT *, LAST_DAY(end_date) AS end_date_month_end
    FROM subscriptions
    WHERE monthly_fee >= 70
),
plan_summary AS (
    SELECT plan, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range, SUM(monthly_fee) AS total_monthly_fee
    FROM base
    GROUP BY plan
)
SELECT plan, monthly_fee_range, total_monthly_fee
FROM plan_summary
WHERE monthly_fee_range >= 68;
