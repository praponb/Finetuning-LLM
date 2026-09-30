SELECT
    QUARTER(c.signup_date) AS signup_quarter,
    c.city,
    ROUND(AVG(s.monthly_fee), 2) AS avg_monthly_fee,
    MIN(s.start_date) AS first_start_date
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
GROUP BY QUARTER(c.signup_date), c.city
HAVING ROUND(AVG(s.monthly_fee), 2) > 60
ORDER BY first_start_date DESC, signup_quarter, city
LIMIT 5
