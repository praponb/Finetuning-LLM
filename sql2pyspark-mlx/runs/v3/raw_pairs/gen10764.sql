SELECT
    budget,
    dept_name,
    location,
    CASE WHEN dept_name = 'Legal' THEN 1 ELSE 0 END AS is_legal
FROM departments
WHERE dept_name NOT IN ('Marketing', 'Engineering', 'Finance')
ORDER BY budget DESC
