SELECT
    DATE_TRUNC('YEAR', start_date) AS year_start,
    COUNT(CASE WHEN plan = 'premium' THEN 1 END) AS premium_count,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee,
    COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count
FROM subscriptions
GROUP BY DATE_TRUNC('YEAR', start_date)
ORDER BY year_start DESC;
