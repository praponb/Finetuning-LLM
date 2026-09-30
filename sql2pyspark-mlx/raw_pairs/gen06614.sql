WITH valid_subscriptions AS (
    SELECT *, INITCAP(TRIM(plan)) AS plan_title
    FROM subscriptions
    WHERE customer_id IN (8, 14, 30)
),
Agg AS (
    SELECT YEAR(end_date) AS end_year, COUNT(DISTINCT monthly_fee) AS unique_plans, SUM(CASE WHEN plan = 'premium' THEN monthly_fee ELSE 0 END) AS premium_monthly_fee
    FROM valid_subscriptions
    GROUP BY YEAR(end_date)
)
SELECT *
FROM Agg
WHERE unique_plans >= 5
ORDER BY premium_monthly_fee DESC
