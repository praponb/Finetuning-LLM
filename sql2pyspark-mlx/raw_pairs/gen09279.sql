SELECT
    departments.dept_name,
    departments.location,
    departments.budget,
    employees.salary,
    employees.level
FROM departments
JOIN employees ON employees.dept_id = departments.id
WHERE employees.job_title = 'Engineer' AND departments.location <> 'Berlin'
ORDER BY employees.level
