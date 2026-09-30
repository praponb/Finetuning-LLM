SELECT job_title, level, CONCAT(email, ' ', full_name) AS email_full_name, COALESCE(full_name, 'unassigned') AS full_name_filled FROM employees ORDER BY email_full_name, job_title
