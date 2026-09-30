SELECT id, full_name, salary
FROM employees
WHERE manager_id = 2
ORDER BY full_name, id
LIMIT 50
