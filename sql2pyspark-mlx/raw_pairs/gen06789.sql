SELECT id, email, CASE WHEN is_remote THEN 'Yes' ELSE 'No' END is_remote_label
FROM employees
WHERE NOT is_remote AND dept_id IN (5, 20, 29) AND is_remote = TRUE
