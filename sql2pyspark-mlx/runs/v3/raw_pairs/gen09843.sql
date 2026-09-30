SELECT
    DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    MAX(credit_limit) AS max_credit_limit,
    COUNT(DISTINCT age) AS unique_segments
FROM customers
GROUP BY DATE_FORMAT(signup_date, 'yyyy-MM')
HAVING avg_credit_limit > 53
ORDER BY 4;
