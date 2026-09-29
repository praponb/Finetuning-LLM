SELECT email, QUARTER(hire_date) AS hire_date_quarter, ROUND(salary, 0) AS salary_rounded
FROM employees
WHERE email LIKE '%@startup.io' AND level = 'L5';
