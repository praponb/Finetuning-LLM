SELECT
    c.id,
    c.segment,
    COUNT(s.id) AS subscription_count,
    ROUND(AVG(s.monthly_fee), 2) AS avg_monthly_fee
FROM customers c
LEFT JOIN subscriptions s ON c.id = s.customer_id
WHERE YEAR(c.signup_date) = 2023
GROUP BY c.id, c.segment
HAVING ROUND(AVG(s.monthly_fee), 2) >= 21
ORDER BY id, avg_monthly_fee, segment
LIMIT 10;
