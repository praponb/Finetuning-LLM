SELECT * FROM (SELECT id, job_title, hire_date, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date, id) AS pos FROM employees) AS ranked WHERE pos <= 3;
