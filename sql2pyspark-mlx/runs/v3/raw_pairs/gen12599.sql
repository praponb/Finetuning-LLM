SELECT
    country,
    CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort,
    MAX(credit_limit) AS max_credit_limit,
    COUNT(*) AS num_customers,
    MIN(age) AS min_age
FROM customers
WHERE age BETWEEN 44 AND 46
GROUP BY country, signup_cohort
ORDER BY country;
