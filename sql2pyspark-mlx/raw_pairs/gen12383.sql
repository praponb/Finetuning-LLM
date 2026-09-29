SELECT CASE WHEN monthly_fee >= 183 THEN 'high' WHEN monthly_fee >= 104 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket, AVG(monthly_fee) AS avg_monthly_fee, SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM subscriptions
WHERE status IN ('CANCELLED', 'PAUSED', 'EXPIRED')
GROUP BY 1
HAVING avg_monthly_fee > 170
