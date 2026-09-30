SELECT manager_id FROM employees WHERE is_remote = TRUE UNION ALL SELECT manager_id FROM employees WHERE is_remote ORDER BY manager_id DESC
