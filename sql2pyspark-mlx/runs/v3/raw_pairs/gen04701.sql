SELECT
    employees.id,
    employees.full_name,
    employees.hire_date,
    employees.email
FROM employees
WHERE employees.dept_id IN (SELECT id FROM departments WHERE dept_name NOT IN ('Engineering', 'Sales'))
ORDER BY employees.id DESC, hire_date;
