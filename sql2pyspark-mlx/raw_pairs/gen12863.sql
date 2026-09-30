SELECT *, CASE WHEN is_remote THEN 'Yes' ELSE 'No' END is_remote_label FROM employees WHERE is_remote = TRUE
