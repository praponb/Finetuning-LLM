SELECT YEAR(hire_date) hire_year, MAX(salary) max_salary
FROM employees
WHERE is_remote = TRUE
GROUP BY YEAR(hire_date)
ORDER BY hire_year ASC, max_salary DESC
