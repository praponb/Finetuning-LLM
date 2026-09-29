WITH src AS (
    SELECT *
    FROM subscriptions
    WHERE status = 'PAUSED'
),
plan_group_summary AS (
    SELECT CASE WHEN plan IN ('basic', 'premium') THEN 'tier_1' ELSE 'other' END AS plan_group, SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_count
    FROM src
    GROUP BY CASE WHEN plan IN ('basic', 'premium') THEN 'tier_1' ELSE 'other' END
)
SELECT *
FROM plan_group_summary
ORDER BY cancelled_count DESC, plan_group
LIMIT 20
