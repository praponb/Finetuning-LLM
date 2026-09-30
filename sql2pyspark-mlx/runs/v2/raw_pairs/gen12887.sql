SELECT customer_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values, SUM(CASE WHEN status = 'CANCELLED' THEN monthly_fee ELSE 0 END) AS cancelled_monthly_fee
FROM subscriptions
WHERE status IN ('CANCELLED', 'ACTIVE')
GROUP BY customer_id
HAVING cancelled_monthly_fee < 77;
