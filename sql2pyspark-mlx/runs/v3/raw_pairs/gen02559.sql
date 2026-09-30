SELECT id, job_title, salary, email FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary DESC, id) rnk FROM employees) r WHERE rnk <= 3 ORDER BY id DESC;
