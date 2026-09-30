SELECT MONTH(hire_date) hire_month, AVG(salary) avg_salary
FROM employees
GROUP BY MONTH(hire_date);
