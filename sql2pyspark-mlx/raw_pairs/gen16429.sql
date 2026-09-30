SELECT e.id, e.full_name, d.budget, d.location, d.dept_name, CAST(d.budget AS INT) AS budget_int FROM employees e JOIN departments d ON e.dept_id = d.id WHERE e.manager_id = 3;
