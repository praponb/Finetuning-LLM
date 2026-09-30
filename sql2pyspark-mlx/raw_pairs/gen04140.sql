SELECT *, CASE WHEN dept_name = 'Engineering' THEN 1 ELSE 0 END AS is_engineering
FROM departments
WHERE dept_name NOT IN ('Legal', 'Engineering')
