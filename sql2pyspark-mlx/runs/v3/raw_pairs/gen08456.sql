SELECT *, COALESCE(job_title, 'Unknown') AS job_title_filled, UPPER(TRIM(full_name)) AS full_name_normalized FROM employees WHERE is_remote AND email LIKE '%@mail.co.th' AND YEAR(hire_date) >= 2023
