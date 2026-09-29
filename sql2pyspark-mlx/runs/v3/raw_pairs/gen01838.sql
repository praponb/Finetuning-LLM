SELECT
    status,
    plan,
    MONTH(end_date) AS end_month,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(plan))) AS plan_values,
    AVG(monthly_fee) AS avg_monthly_fee,
    COUNT(*) AS subscription_count
FROM subscriptions
GROUP BY status, plan, MONTH(end_date)
