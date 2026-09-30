WITH recent_subscriptions AS (
    SELECT *
    FROM subscriptions
    WHERE plan IN ('premium', 'enterprise', 'basic')
),
Agg AS (
    SELECT CASE WHEN plan IN ('enterprise', 'premium') THEN 'priority' ELSE 'other' END plan_group, SORT_ARRAY(COLLECT_SET(plan)) plan_list, SUM(monthly_fee) total_monthly_fee
    FROM recent_subscriptions
    GROUP BY CASE WHEN plan IN ('enterprise', 'premium') THEN 'priority' ELSE 'other' END
)
SELECT plan_group, plan_list
FROM Agg
ORDER BY plan_group
LIMIT 50
