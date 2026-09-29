SELECT MONTH(end_date) AS end_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
FROM subscriptions
WHERE status <> 'ACTIVE'
GROUP BY MONTH(end_date)
ORDER BY end_month DESC
