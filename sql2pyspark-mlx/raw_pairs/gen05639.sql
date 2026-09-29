SELECT id, job_title, hire_date FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date DESC, id) AS rn FROM employees WHERE job_title IS NULL) AS ranked WHERE rn <= 2;
