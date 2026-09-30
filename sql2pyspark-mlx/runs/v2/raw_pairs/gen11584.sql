SELECT QUARTER(hire_date) AS hire_quarter, DATE_TRUNC('MONTH', hire_date) AS hire_month, MAX(salary) - MIN(salary) AS salary_range
FROM employees
WHERE email LIKE '%@gmail.com' AND email IS NOT NULL
GROUP BY QUARTER(hire_date), DATE_TRUNC('MONTH', hire_date)
HAVING MAX(salary) - MIN(salary) < 57;
