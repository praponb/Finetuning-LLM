SELECT
    MONTH(signup_date) signup_month,
    ROUND(AVG(credit_limit), 2) avg_credit_limit,
    ROUND(STDDEV(age), 2) stddev_age,
    ROUND((100 * SUM(CASE WHEN country = 'US' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_us
FROM customers
GROUP BY MONTH(signup_date)
ORDER BY avg_credit_limit;
