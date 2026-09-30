WITH valid_subscriptions AS (
    SELECT *
    FROM subscriptions
    WHERE status = 'EXPIRED' AND plan NOT IN ('premium', 'basic')
),
PlanSummary AS (
    SELECT plan, MIN(monthly_fee) min_monthly_fee
    FROM valid_subscriptions
    GROUP BY plan
)
SELECT plan, min_monthly_fee
FROM PlanSummary
WHERE min_monthly_fee >= 120
ORDER BY plan
