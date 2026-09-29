WITH employees_by_department AS (
    SELECT dept_id, MAX(hire_date) AS last_hire_date, SUM(salary) AS total_salary
    FROM employees
    WHERE manager_id = 28
    GROUP BY dept_id
)
SELECT
    d.id,
    d.dept_name,
    d.budget,
    ebd.last_hire_date,
    ebd.total_salary
FROM departments d
INNER JOIN employees_by_department ebd ON d.id = ebd.dept_id
ORDER BY ebd.last_hire_date, id DESC;
