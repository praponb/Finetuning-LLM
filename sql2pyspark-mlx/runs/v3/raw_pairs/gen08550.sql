SELECT CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort, MAX(age) - MIN(age) AS age_range, AVG(credit_limit) AS avg_credit_limit
FROM customers
WHERE email LIKE '%@GMAIL.COM' AND email LIKE '%@company.co' AND is_active
GROUP BY signup_cohort;
