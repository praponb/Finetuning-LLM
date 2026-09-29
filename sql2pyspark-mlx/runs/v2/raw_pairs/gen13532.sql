SELECT
    YEAR(signup_date) AS signup_year,
    segment,
    MAX(age) - MIN(age) AS age_range,
    ROUND((100 * SUM(CASE WHEN city = 'New York' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_new_york
FROM customers
WHERE credit_limit BETWEEN 9700 AND 11700 AND LENGTH(name) < 8
GROUP BY YEAR(signup_date), segment
