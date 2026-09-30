SELECT ROUND((100 * SUM(CASE WHEN plan = 'premium' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_premium, SUM(monthly_fee) AS total_monthly_fee, MAX(end_date) AS last_end_date
FROM subscriptions
WHERE end_date > '2024-07-28'
ORDER BY total_monthly_fee, pct_premium DESC;
