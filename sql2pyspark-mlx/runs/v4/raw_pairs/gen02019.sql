SELECT id, email, COALESCE(level, 'unassigned') AS level_filled, CAST(is_remote AS INT) AS is_remote_flag FROM employees WHERE salary > 139000;
