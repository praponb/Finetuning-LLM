SELECT plan, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(plan))) AS plan_values
FROM subscriptions
WHERE status <> 'CANCELLED' AND customer_id IN (4, 30)
GROUP BY plan
