SELECT job_title, is_remote
FROM employees
WHERE full_name IS NOT NULL AND full_name LIKE '%Ross%' AND salary > 102000;
