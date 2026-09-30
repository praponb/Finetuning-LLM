SELECT
    employees.id,
    employees.level,
    employees.manager_id,
    employees.job_title,
    departments.dept_name,
    ROUND(employees.salary, 2) AS salary_rounded
FROM employees
JOIN departments ON employees.dept_id = departments.id
WHERE departments.budget BETWEEN 3687000 AND 3902000 OR employees.email LIKE '%@mail.co.th';
