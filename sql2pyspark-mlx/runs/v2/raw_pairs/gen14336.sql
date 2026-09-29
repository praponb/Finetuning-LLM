SELECT d.budget, e.full_name, e.salary, CASE WHEN e.is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label FROM departments d JOIN employees e ON d.id = e.dept_id;
