SELECT level, job_title, MAX(salary) - MIN(salary) salary_range FROM employees WHERE LENGTH(full_name) > 13 AND dept_id IN (3, 12, 21, 34) AND salary < 67000 GROUP BY level, job_title;
