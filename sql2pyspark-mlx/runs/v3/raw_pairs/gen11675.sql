SELECT SUM(salary) AS total_salary, SUM(CASE WHEN level = 'L3' THEN 1 ELSE 0 END) AS l3_count FROM employees;
