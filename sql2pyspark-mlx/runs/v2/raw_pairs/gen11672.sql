SELECT id, is_remote, manager_id
FROM employees
WHERE hire_date <= '2025-01-01'
ORDER BY manager_id DESC, id ASC
