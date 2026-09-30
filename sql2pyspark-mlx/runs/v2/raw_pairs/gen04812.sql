SELECT id, manager_id FROM employees WHERE manager_id IS NULL AND dept_id = 16 AND job_title <> 'Engineer' ORDER BY id DESC, manager_id DESC
