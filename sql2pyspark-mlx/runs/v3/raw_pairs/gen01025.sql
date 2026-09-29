SELECT
    CASE WHEN monthly_fee >= 230 THEN 'high' WHEN monthly_fee >= 62 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket,
    status,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
FROM subscriptions
GROUP BY CASE WHEN monthly_fee >= 230 THEN 'high' WHEN monthly_fee >= 62 THEN 'mid' ELSE 'low' END, status
HAVING COUNT(monthly_fee) >= 1
ORDER BY status, monthly_fee_bucket
LIMIT 10;
