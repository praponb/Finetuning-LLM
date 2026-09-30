SELECT e.is_remote AS employee_is_remote, e.manager_id AS employee_manager_id, m.is_remote AS manager_is_remote
FROM employees AS e
LEFT OUTER JOIN employees AS m ON e.manager_id = m.id
WHERE m.full_name NOT LIKE '%Jack%' AND m.is_remote;
