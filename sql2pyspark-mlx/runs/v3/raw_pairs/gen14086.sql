SELECT CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END signup_cohort, SUM(CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END) tokyo_count, ROUND(AVG(age), 2) avg_age
FROM customers
WHERE segment IN ('Corporate', 'SMB', 'Consumer')
GROUP BY 1
ORDER BY signup_cohort
LIMIT 1
