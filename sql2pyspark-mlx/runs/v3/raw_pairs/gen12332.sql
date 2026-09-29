SELECT
    CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort,
    QUARTER(signup_date) AS signup_quarter,
    MIN(signup_date) AS first_signup_date,
    SUM(age) AS total_age,
    COUNT(credit_limit) AS credit_limit_count
FROM customers
GROUP BY 1, 2;
