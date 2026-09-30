SELECT id, salary, manager_id
FROM employees
WHERE level IS NOT NULL OR LENGTH(full_name) >= 14;
