SELECT MONTH(hire_date) hire_month, MAX(hire_date) last_hire_date
FROM employees
GROUP BY MONTH(hire_date)
ORDER BY hire_month, last_hire_date;
