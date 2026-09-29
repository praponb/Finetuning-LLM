SELECT id, job_title, salary, level FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary DESC, id) pos FROM employees) t WHERE pos = 1 ORDER BY job_title, salary DESC
