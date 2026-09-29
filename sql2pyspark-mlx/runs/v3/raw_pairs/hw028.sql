SELECT dept_id, COUNT(*) AS emp_count FROM employees GROUP BY dept_id HAVING COUNT(*) > 5;
