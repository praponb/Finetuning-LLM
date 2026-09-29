SELECT manager_id FROM employees WHERE job_title = 'Manager' INTERSECT SELECT manager_id FROM employees WHERE job_title <> 'Designer' ORDER BY manager_id DESC;
