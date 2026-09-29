SELECT d.budget, e.job_title, e.level
FROM departments d
JOIN employees e ON e.dept_id = d.id
