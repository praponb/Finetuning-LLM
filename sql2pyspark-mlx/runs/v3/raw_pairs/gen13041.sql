SELECT
    customer_id,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(plan))) plan_values,
    SUM(monthly_fee) total_monthly_fee,
    MAX(start_date) latest_start_date
FROM subscriptions
GROUP BY 1
