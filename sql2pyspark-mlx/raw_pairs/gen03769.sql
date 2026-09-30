WITH src AS (
    SELECT *
    FROM subscriptions
    WHERE YEAR(end_date) >= 2025 AND (plan = 'enterprise' OR plan <> 'premium')
),
subscriptions_stats AS (
    SELECT QUARTER(end_date) end_quarter, SORT_ARRAY(COLLECT_SET(plan)) plan_list
    FROM src
    GROUP BY QUARTER(end_date)
)
SELECT end_quarter, plan_list
FROM subscriptions_stats
ORDER BY end_quarter DESC
