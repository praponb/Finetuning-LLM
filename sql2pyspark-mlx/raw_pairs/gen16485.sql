SELECT
    departments.location,
    departments.budget,
    employees.job_title,
    LOWER(departments.dept_name) AS dept_name_lower
FROM departments
LEFT JOIN employees ON employees.dept_id = departments.id
WHERE employees.manager_id IS NULL AND departments.dept_name IN ('Marketing', 'Engineering');
