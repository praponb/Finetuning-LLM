SELECT DATE_TRUNC('MONTH', hire_date) AS hire_month, SUM(salary) AS total_salary, SUM(SUM(salary)) OVER (ORDER BY DATE_TRUNC('MONTH', hire_date)) AS cumulative_salary
FROM employees
GROUP BY DATE_TRUNC('MONTH', hire_date)
ORDER BY cumulative_salary DESC, total_salary, hire_month
LIMIT 1
