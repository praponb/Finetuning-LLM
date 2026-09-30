SELECT id, email, manager_id
FROM employees
WHERE level IN ('L3', 'L5')
ORDER BY manager_id, email, id
LIMIT 1
