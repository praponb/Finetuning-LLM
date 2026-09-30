SELECT
    segment,
    YEAR(signup_date) AS signup_year,
    MIN(signup_date) AS first_signup_date,
    ROUND((100 * SUM(CASE WHEN country = 'UK' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_uk
FROM customers
GROUP BY segment, YEAR(signup_date)
HAVING COUNT(*) >= 1;
