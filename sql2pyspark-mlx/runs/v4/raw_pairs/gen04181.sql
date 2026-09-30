SELECT DATE_TRUNC('MONTH', hire_date) hire_month, AVG(salary) avg_salary, MIN(salary) min_salary
FROM employees
WHERE full_name LIKE '%Dav%' AND (level = 'L3' OR hire_date < '2025-06-29')
GROUP BY DATE_TRUNC('MONTH', hire_date)
ORDER BY min_salary ASC
