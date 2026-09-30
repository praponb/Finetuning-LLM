SELECT * FROM (SELECT id, job_title, hire_date, manager_id, DENSE_RANK() OVER (PARTITION BY job_title ORDER BY hire_date DESC) AS pos FROM employees) AS t WHERE pos <= 2;
