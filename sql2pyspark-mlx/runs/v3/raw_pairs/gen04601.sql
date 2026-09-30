SELECT id, email, job_title FROM employees WHERE LOWER(full_name) LIKE '%ob %' EXCEPT SELECT id, email, job_title FROM employees WHERE job_title = 'Director' ORDER BY id DESC;
