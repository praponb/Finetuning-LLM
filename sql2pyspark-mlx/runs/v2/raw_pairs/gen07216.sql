SELECT id, full_name, manager_id
FROM employees
WHERE manager_id IS NOT NULL
ORDER BY full_name, manager_id DESC, id
LIMIT 10
