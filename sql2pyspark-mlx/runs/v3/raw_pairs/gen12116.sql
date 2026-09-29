SELECT
    departments.budget,
    employees.job_title,
    employees.hire_date,
    employees.is_remote,
    CONCAT_WS('-', employees.full_name, employees.level) AS full_name_level_key
FROM departments
LEFT JOIN employees ON employees.dept_id = departments.id
WHERE employees.manager_id IS NULL
ORDER BY full_name_level_key;
