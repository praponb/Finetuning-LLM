SELECT ROUND(AVG(salary), 2) AS avg_salary
FROM employees
WHERE (level = 'L3' AND job_title <> 'Designer') OR full_name IS NOT NULL
ORDER BY avg_salary DESC
