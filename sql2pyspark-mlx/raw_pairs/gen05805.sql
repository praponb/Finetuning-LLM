SELECT status, SUM(CASE WHEN plan = 'basic' THEN monthly_fee ELSE 0 END) basic_monthly_fee, MAX(end_date) latest_end_date
FROM subscriptions
WHERE monthly_fee BETWEEN 100 AND 230 AND customer_id IN (1, 27, 37, 38)
GROUP BY status
ORDER BY latest_end_date, status
LIMIT 5;
