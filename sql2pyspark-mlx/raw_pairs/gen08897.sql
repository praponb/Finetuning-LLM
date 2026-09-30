SELECT salary, manager_id, dept_id FROM employees WHERE job_title IN ('Analyst', 'Designer', 'Director') AND (job_title = 'Analyst' OR level NOT IN ('L5', 'L3', 'L2')) ORDER BY 2
