WITH department_stats AS (
    SELECT dept_id, MIN(salary) AS min_salary, MIN(hire_date) AS first_hire_date
    FROM employees
    WHERE salary >= 148000
    GROUP BY dept_id
)
SELECT d.budget, ds.min_salary, ds.first_hire_date
FROM departments d
LEFT JOIN department_stats ds ON d.id = ds.dept_id
WHERE ds.min_salary > 75000;
