SELECT id, level, salary, job_title FROM (SELECT id, level, salary, job_title, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary DESC, id) AS rn FROM employees) AS ranked WHERE rn <= 5
