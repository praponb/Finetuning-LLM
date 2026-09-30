SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(plan))) AS plan_values, MAX(monthly_fee) AS max_monthly_fee, SUM(CASE WHEN status = 'EXPIRED' THEN monthly_fee ELSE 0 END) AS expired_monthly_fee
FROM subscriptions
WHERE monthly_fee <= 200
ORDER BY expired_monthly_fee ASC;
