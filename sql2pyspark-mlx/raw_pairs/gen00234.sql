SELECT id, manager_id, CASE WHEN is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label
FROM employees;
