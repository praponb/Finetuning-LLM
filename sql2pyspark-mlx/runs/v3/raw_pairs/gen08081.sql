SELECT id, job_title, hire_date FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date, id) AS pos FROM employees) AS r WHERE pos <= 2 ORDER BY hire_date DESC;
