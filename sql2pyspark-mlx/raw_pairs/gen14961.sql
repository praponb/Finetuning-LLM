SELECT
    plan,
    status,
    MIN(monthly_fee) AS min_monthly_fee,
    MAX(start_date) AS last_start_date,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(plan))) AS plan_values
FROM subscriptions
GROUP BY plan, status
ORDER BY plan
