SELECT MONTH(hire_date) AS hire_month, MAX(salary) AS max_salary
FROM employees
GROUP BY hire_month
ORDER BY max_salary, hire_month DESC;
