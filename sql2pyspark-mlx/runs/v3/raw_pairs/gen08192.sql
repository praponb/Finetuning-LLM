SELECT CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END AS hire_cohort, CASE WHEN salary >= 138000 THEN 'high' WHEN salary >= 75000 THEN 'mid' ELSE 'low' END AS salary_bucket, ROUND(AVG(salary), 2) AS avg_salary
FROM employees
WHERE salary BETWEEN 118000 AND 137000 AND manager_id IN (5, 11)
GROUP BY hire_cohort, salary_bucket
HAVING avg_salary > 25
ORDER BY salary_bucket DESC, hire_cohort;
