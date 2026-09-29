SELECT id, full_name, COALESCE(email, 'N/A') AS email_filled, COALESCE(email, 'unassigned') AS email_filled_2 FROM employees WHERE job_title NOT IN ('Director', 'Designer', 'Intern')
