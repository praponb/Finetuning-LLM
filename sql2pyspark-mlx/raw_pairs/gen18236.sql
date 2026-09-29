SELECT id, salary, manager_id, FIRST_VALUE(manager_id) OVER (ORDER BY salary DESC, id) AS first_manager_id FROM employees;
