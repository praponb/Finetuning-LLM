SELECT QUARTER(hire_date) AS hire_quarter, AVG(salary) AS avg_salary, SUM(CASE WHEN level = 'L5' THEN 1 ELSE 0 END) AS l5_count
FROM employees
GROUP BY QUARTER(hire_date);
