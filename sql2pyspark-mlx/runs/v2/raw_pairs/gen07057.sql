SELECT d.location, d.budget, e.is_remote
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
WHERE d.dept_name NOT IN ('Marketing', 'Sales', 'HR') AND d.location = 'London'
