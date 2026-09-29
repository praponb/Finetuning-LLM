SELECT dept_id FROM employees WHERE full_name IS NOT NULL UNION ALL SELECT dept_id FROM employees WHERE job_title IN ('Director', 'Designer') ORDER BY dept_id
