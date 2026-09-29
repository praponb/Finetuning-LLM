SELECT MIN(hire_date) AS min_hire_date, MAX(salary) AS max_salary
FROM employees
WHERE level NOT IN ('L2', 'L4', 'L5') AND job_title IN ('Intern', 'Director', 'Engineer')
ORDER BY max_salary DESC;
