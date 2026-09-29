SELECT is_remote, CASE WHEN level = 'L4' THEN 1 ELSE 0 END AS is_l4, CASE WHEN is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label
FROM employees
WHERE salary > 61000
