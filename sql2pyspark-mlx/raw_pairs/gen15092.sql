SELECT CASE WHEN status IN ('CANCELLED', 'PAUSED') THEN 'tier_1' ELSE 'other' END AS status_group, MONTH(start_date) AS start_month, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
FROM subscriptions
WHERE customer_id IS NULL AND (MONTH(start_date) IN (1, 3, 12) OR start_date >= '2024-02-26')
GROUP BY ROLLUP(CASE WHEN status IN ('CANCELLED', 'PAUSED') THEN 'tier_1' ELSE 'other' END, MONTH(start_date))
ORDER BY 2 DESC, status_group;
