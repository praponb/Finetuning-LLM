WITH RecentSubscriptions AS (
    SELECT *, CONCAT_WS('-', status, plan) AS status_plan_key
    FROM subscriptions
    WHERE status NOT IN ('EXPIRED', 'CANCELLED', 'ACTIVE') AND (customer_id IN (4, 29, 35) OR plan = 'enterprise')
),
subscriptions_stats AS (
    SELECT QUARTER(end_date) AS end_quarter, YEAR(end_date) AS end_year, COUNT(DISTINCT monthly_fee) AS unique_plans
    FROM RecentSubscriptions
    GROUP BY QUARTER(end_date), YEAR(end_date)
)
SELECT *
FROM subscriptions_stats
WHERE unique_plans > 8
ORDER BY unique_plans, end_quarter, end_year
LIMIT 5;
