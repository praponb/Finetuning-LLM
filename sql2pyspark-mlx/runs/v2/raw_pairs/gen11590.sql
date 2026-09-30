SELECT *, CASE WHEN is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label
FROM employees
WHERE level IN ('L3', 'L4', 'L2') AND hire_date < '2025-05-04'
