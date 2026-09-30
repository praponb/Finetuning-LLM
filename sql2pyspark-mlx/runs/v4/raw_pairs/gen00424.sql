SELECT id, hire_date, email, is_remote FROM employees WHERE hire_date >= (SELECT MAX(hire_date) FROM employees WHERE job_title NOT IN ('Designer', 'Manager', 'Director')) ORDER BY id ASC, email ASC;
