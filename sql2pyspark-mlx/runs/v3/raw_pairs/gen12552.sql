SELECT d.budget, e.level
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id
WHERE e.salary > 147000 AND d.budget > 1692000;
