SELECT DATE_TRUNC('MONTH', hire_date) AS hire_month, AVG(salary) AS avg_salary
FROM employees
WHERE email LIKE '%@company.co' AND level IN ('L1', 'L4')
GROUP BY DATE_TRUNC('MONTH', hire_date)
