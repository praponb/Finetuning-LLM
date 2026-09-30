SELECT id, manager_id, salary, full_name, FIRST_VALUE(full_name) OVER (PARTITION BY manager_id ORDER BY salary, id) first_full_name FROM employees ORDER BY id DESC, full_name DESC
