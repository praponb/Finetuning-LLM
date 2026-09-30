SELECT QUARTER(end_date) AS end_quarter, SUM(CASE WHEN plan = 'premium' THEN 1 ELSE 0 END) AS premium_count
FROM subscriptions
WHERE plan IN ('enterprise', 'premium', 'standard')
GROUP BY QUARTER(end_date)
ORDER BY end_quarter DESC, premium_count
LIMIT 1;
