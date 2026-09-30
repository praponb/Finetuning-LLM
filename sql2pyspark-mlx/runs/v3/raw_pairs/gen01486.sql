SELECT YEAR(hire_date) AS hire_year, MIN(salary) AS min_salary, ROUND(AVG(salary), 2) AS avg_salary
FROM employees
WHERE job_title IN ('Intern', 'Director', 'Designer') AND is_remote
GROUP BY 1
