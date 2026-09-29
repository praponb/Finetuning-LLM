SELECT
    departments.dept_name,
    departments.budget,
    employees.is_remote,
    employees.job_title,
    employees.manager_id,
    CAST(employees.is_remote AS INT) AS is_remote_flag
FROM departments
INNER JOIN employees ON departments.id = employees.dept_id;
