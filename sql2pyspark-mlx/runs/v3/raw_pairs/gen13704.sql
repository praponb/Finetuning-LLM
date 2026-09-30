WITH department_summary AS (
    SELECT dept_id, ROUND(AVG(salary), 2) AS avg_salary, COUNT(*) AS employee_count
    FROM employees
    WHERE is_remote = TRUE
    GROUP BY dept_id
)
SELECT
    d.id,
    d.budget,
    d.dept_name,
    ds.avg_salary,
    ds.employee_count
FROM departments d
JOIN department_summary ds ON d.id = ds.dept_id;
