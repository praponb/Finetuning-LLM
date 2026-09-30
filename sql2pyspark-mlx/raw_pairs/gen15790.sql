SELECT id, job_title, hire_date FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date DESC, id) rnk FROM employees WHERE LOWER(TRIM(full_name)) = 'henry ford') r WHERE rnk = 1
