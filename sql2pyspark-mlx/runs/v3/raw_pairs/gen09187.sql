SELECT salary, dept_id, CASE WHEN is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label, TRIM(full_name) AS clean_full_name FROM employees WHERE salary BETWEEN 121000 AND 142000
