SELECT d.dept_name, d.budget, e.hire_date
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id
WHERE e.full_name LIKE '%smit%' AND e.job_title = 'Manager';
