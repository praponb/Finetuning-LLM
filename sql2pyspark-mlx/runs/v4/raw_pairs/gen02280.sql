SELECT job_title FROM employees WHERE manager_id IN (5, 10, 20) INTERSECT SELECT job_title FROM employees WHERE manager_id IS NULL ORDER BY job_title;
