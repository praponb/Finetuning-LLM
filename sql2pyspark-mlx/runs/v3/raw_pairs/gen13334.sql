SELECT DATE_TRUNC('MONTH', signup_date) AS signup_month, CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort, SUM(CASE WHEN country = 'US' THEN credit_limit ELSE 0 END) AS us_credit_limit
FROM customers
WHERE MONTH(signup_date) IN (3, 7, 8)
GROUP BY 1, 2;
