WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM subscriptions
    QUALIFY RANK() OVER (PARTITION BY customer_id, plan ORDER BY start_date DESC) = 1
)
-- Aggregate with conditional metrics
SELECT
    plan,
    MAX(monthly_fee) FILTER (WHERE plan = 'premium') AS premium_max_monthly_fee,
    SUM(CASE WHEN plan = 'premium' THEN monthly_fee ELSE 0 END) AS premium_monthly_fee,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM ranked
GROUP BY 1
