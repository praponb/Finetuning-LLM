WITH employees_by_department AS (
    SELECT dept_id, MAX(hire_date) AS last_hire_date
    FROM employees
    WHERE job_title IS NULL
    GROUP BY dept_id
)
SELECT d.budget, d.dept_name, ebd.last_hire_date
FROM departments d
INNER JOIN employees_by_department ebd ON d.id = ebd.dept_id
