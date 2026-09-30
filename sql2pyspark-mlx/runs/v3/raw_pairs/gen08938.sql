SELECT
    DATE_FORMAT(end_date, 'yyyy-MM') AS end_month,
    MAX(start_date) AS last_start_date,
    COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count,
    SUM(monthly_fee) AS total_monthly_fee
FROM subscriptions
GROUP BY DATE_FORMAT(end_date, 'yyyy-MM')
HAVING COUNT(CASE WHEN plan = 'standard' THEN 1 END) > 3
ORDER BY enterprise_count
