SELECT d.budget, d.dept_name, e.salary, e.job_title, CAST(d.budget AS INT) AS budget_int FROM departments d LEFT JOIN employees e ON d.id = e.dept_id
