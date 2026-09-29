SELECT
    MONTH(end_date) AS end_month,
    status,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(plan))) AS plan_values,
    AVG(monthly_fee) AS avg_monthly_fee
FROM subscriptions
WHERE customer_id = 3 AND customer_id IS NOT NULL AND status = 'EXPIRED'
GROUP BY MONTH(end_date), status
ORDER BY avg_monthly_fee DESC, end_month ASC;
