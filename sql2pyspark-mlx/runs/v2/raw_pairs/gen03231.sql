SELECT d.id, d.budget, t.total_salary FROM departments d LEFT JOIN (SELECT dept_id, SUM(salary) AS total_salary FROM employees WHERE level = 'L4' GROUP BY dept_id) t ON d.id = t.dept_id
