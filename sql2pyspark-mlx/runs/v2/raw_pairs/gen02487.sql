SELECT
    DATE_FORMAT(c.signup_date, 'yyyy-MM') signup_month,
    QUARTER(c.signup_date) signup_quarter,
    COUNT(DISTINCT s.monthly_fee) unique_plans,
    SUM(CASE WHEN s.plan = 'premium' THEN s.monthly_fee ELSE 0 END) premium_monthly_fee
FROM subscriptions s
LEFT JOIN customers c ON s.customer_id = c.id
GROUP BY DATE_FORMAT(c.signup_date, 'yyyy-MM'), QUARTER(c.signup_date)
ORDER BY premium_monthly_fee DESC, signup_month, signup_quarter
LIMIT 10
