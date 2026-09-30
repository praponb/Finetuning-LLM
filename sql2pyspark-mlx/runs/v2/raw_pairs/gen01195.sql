SELECT job_title, email, manager_id, INITCAP(TRIM(level)) AS level_title FROM employees WHERE job_title IN ('Designer', 'Intern') AND (full_name LIKE '%Mia%' OR salary >= 97000) ORDER BY level_title
