SELECT customer_id, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee, SUM(CASE WHEN status = 'PAUSED' THEN monthly_fee ELSE 0 END) AS paused_monthly_fee
FROM subscriptions
WHERE start_date >= '2025-03-01'
GROUP BY customer_id
ORDER BY paused_monthly_fee DESC;
