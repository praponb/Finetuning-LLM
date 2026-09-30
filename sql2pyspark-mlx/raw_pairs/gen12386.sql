SELECT id, email, dept_id, is_remote FROM employees QUALIFY ROW_NUMBER() OVER (PARTITION BY level, manager_id ORDER BY salary DESC, id) = 1;
