SELECT id, manager_id
FROM employees
WHERE manager_id IN (10, 21, 38) AND manager_id = 8
ORDER BY manager_id DESC
