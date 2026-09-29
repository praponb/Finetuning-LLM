SELECT job_title, full_name, is_remote, CASE WHEN is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label FROM employees WHERE email NOT LIKE '%@startup.io' AND email LIKE '%@outlook.com';
