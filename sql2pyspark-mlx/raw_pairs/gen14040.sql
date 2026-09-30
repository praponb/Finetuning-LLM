SELECT level, id FROM employees INTERSECT ALL SELECT level, id FROM employees WHERE NOT is_remote
