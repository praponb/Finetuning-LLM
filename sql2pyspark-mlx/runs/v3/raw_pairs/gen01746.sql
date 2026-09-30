WITH recent_subscriptions AS (
    SELECT *, LOWER(status) AS status_lower
    FROM subscriptions
    WHERE customer_id IS NOT NULL OR start_date <= '2025-07-01'
),
agg AS (
    SELECT
        customer_id,
        plan,
        MIN(end_date) AS min_end_date,
        SORT_ARRAY(COLLECT_SET(plan)) AS plan_list
    FROM recent_subscriptions
    GROUP BY customer_id, plan
)
SELECT *
FROM agg
ORDER BY customer_id DESC, plan DESC;
