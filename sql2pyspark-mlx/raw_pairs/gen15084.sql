SELECT DATE_TRUNC('MONTH', start_date) start_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(plan))) plan_values, SUM(monthly_fee) total_monthly_fee
FROM subscriptions
GROUP BY DATE_TRUNC('MONTH', start_date)
ORDER BY total_monthly_fee DESC
