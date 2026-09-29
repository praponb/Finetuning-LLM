SELECT
    plan,
    DATE_FORMAT(start_date, 'yyyy-MM') AS start_month,
    COUNT(*) AS subscription_count,
    COUNT(DISTINCT monthly_fee) AS unique_customers
FROM subscriptions
WHERE YEAR(start_date) = 2023
GROUP BY plan, DATE_FORMAT(start_date, 'yyyy-MM');
