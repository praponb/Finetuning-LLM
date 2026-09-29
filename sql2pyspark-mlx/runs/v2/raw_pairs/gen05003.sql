SELECT dept_id, SUM(salary) AS total_salary, DENSE_RANK() OVER (ORDER BY SUM(salary) DESC) AS rnk FROM employees WHERE hire_date < '2023-09-01' GROUP BY dept_id ORDER BY 1;
