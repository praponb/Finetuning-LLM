SELECT plan, MIN(monthly_fee) AS min_monthly_fee, ROUND((100 * SUM(CASE WHEN plan = 'enterprise' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_enterprise
FROM subscriptions
WHERE end_date BETWEEN '2023-08-01' AND '2023-10-26'
GROUP BY plan;
