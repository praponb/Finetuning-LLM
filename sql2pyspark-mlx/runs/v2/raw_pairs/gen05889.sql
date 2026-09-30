WITH filtered_subscriptions AS (
    SELECT *
    FROM subscriptions
    WHERE status = 'EXPIRED'
),
start_year_summary AS (
    SELECT YEAR(start_date) start_year, COUNT(CASE WHEN plan = 'premium' THEN 1 END) premium_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(plan))) plan_values
    FROM filtered_subscriptions
    GROUP BY YEAR(start_date)
)
SELECT *
FROM start_year_summary
WHERE premium_count >= 6
ORDER BY premium_count;
