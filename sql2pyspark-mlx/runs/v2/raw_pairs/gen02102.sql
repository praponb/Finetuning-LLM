WITH department_summary AS (
    SELECT dept_id, AVG(salary) AS avg_salary, MAX(salary) AS max_salary
    FROM employees
    GROUP BY dept_id
)
SELECT d.location, ds.avg_salary, ds.max_salary
FROM departments d
LEFT JOIN department_summary ds ON d.id = ds.dept_id
WHERE ds.avg_salary >= 148000
ORDER BY avg_salary DESC
