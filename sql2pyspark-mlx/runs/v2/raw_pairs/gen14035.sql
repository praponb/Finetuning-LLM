SELECT d.id, d.location
FROM departments d
WHERE d.id IN (SELECT dept_id FROM employees WHERE NOT is_remote)
ORDER BY d.id, 2;
