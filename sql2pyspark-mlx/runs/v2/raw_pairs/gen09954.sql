SELECT ROUND((100 * SUM(CASE WHEN plan = 'standard' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_standard, MAX(start_date) AS last_start_date
FROM subscriptions;
