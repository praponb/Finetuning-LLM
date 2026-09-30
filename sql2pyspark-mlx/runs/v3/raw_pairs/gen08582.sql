SELECT id, level, job_title, salary, MIN(salary) OVER (PARTITION BY level, job_title) AS level_min_salary FROM employees WHERE level != 'L3' ORDER BY level;
