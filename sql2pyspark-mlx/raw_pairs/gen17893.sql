SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary DESC, id) AS row_num FROM employees) t WHERE row_num = 1;
