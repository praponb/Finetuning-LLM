SELECT CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END AS hire_cohort, ROUND(STDDEV(salary), 2) AS stddev_salary, MAX(salary) AS max_salary
FROM employees
WHERE dept_id = 29 AND is_remote AND LOWER(email) LIKE '%mma%'
GROUP BY 1
ORDER BY hire_cohort;
