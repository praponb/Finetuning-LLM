SELECT d.location, d.budget, e.hire_date, e.manager_id, e.email FROM departments d LEFT OUTER JOIN employees e ON e.dept_id = d.id WHERE d.dept_name IS NOT NULL;
