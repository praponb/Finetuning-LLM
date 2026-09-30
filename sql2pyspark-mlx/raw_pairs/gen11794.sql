WITH employees_by_department AS (
    SELECT dept_id, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY dept_id
)
SELECT d.id, d.location, ebd.avg_salary
FROM departments d
INNER JOIN employees_by_department ebd ON d.id = ebd.dept_id
ORDER BY location, id
LIMIT 1;
