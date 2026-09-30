SELECT YEAR(hire_date) AS hire_year, CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END AS hire_cohort, MIN(salary) AS min_salary
FROM employees
GROUP BY hire_year, hire_cohort
ORDER BY hire_cohort, hire_year
LIMIT 1
