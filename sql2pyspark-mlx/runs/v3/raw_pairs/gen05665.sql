SELECT
    DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month,
    COUNT(*) AS total_count,
    SUM(credit_limit) AS total_credit_limit,
    ROUND(STDDEV(age), 2) AS stddev_age
FROM customers
GROUP BY DATE_FORMAT(signup_date, 'yyyy-MM')
HAVING ROUND(STDDEV(age), 2) >= 48;
