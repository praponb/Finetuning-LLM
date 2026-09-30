SELECT
    MONTH(c.signup_date) AS signup_month,
    c.city,
    COUNT(CASE WHEN s.status = 'ACTIVE' THEN 1 END) AS active_count,
    COUNT(*) AS num_subscriptions
FROM subscriptions s
FULL OUTER JOIN customers c ON c.id = s.customer_id
GROUP BY MONTH(c.signup_date), c.city
ORDER BY active_count DESC;
