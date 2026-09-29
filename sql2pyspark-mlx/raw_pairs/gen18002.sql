WITH department_summary AS (
    SELECT dept_id, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY dept_id
)
SELECT d.budget, ds.avg_salary
FROM departments d
INNER JOIN department_summary ds ON d.id = ds.dept_id;
