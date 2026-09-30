WITH employees_by_department AS (
    SELECT dept_id, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY dept_id
)
SELECT
    departments.id,
    departments.dept_name,
    departments.budget,
    employees_by_department.avg_salary
FROM departments
JOIN employees_by_department ON departments.id = employees_by_department.dept_id;
