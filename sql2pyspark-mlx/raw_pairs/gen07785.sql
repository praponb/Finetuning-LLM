SELECT
    DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month,
    YEAR(signup_date) AS signup_year,
    MAX(age) - MIN(age) AS age_range,
    COUNT(CASE WHEN city = 'Bangkok' THEN 1 END) AS bangkok_count,
    SUM(credit_limit) AS total_credit_limit
FROM customers
WHERE NOT is_active
GROUP BY CUBE(DATE_FORMAT(signup_date, 'yyyy-MM'), YEAR(signup_date))
ORDER BY signup_month DESC
