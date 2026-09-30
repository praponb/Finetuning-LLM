WITH employees_by_department AS (
    SELECT dept_id, MAX(hire_date) AS latest_hire_date
    FROM employees
    GROUP BY dept_id
)
SELECT departments.id, departments.budget, employees_by_department.latest_hire_date
FROM departments
INNER JOIN employees_by_department ON departments.id = employees_by_department.dept_id
ORDER BY employees_by_department.latest_hire_date DESC, id
LIMIT 3
