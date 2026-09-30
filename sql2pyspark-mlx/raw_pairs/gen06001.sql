SELECT
    employees.hire_date,
    employees.email,
    departments.location,
    CASE WHEN employees.salary >= 122000 THEN 'High' WHEN employees.salary >= 67000 THEN 'Medium' ELSE 'Low' END AS salary_level
FROM employees
INNER JOIN departments ON employees.dept_id = departments.id
WHERE departments.location = 'Berlin' AND departments.budget BETWEEN 1093000 AND 3162000
ORDER BY salary_level, hire_date DESC;
