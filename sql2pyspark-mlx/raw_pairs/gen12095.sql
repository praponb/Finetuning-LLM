WITH department_totals AS (
    SELECT dept_id, AVG(salary) AS avg_salary, ROUND(STDDEV(salary), 2) AS stddev_salary
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.location,
    d.budget,
    dt.avg_salary,
    dt.stddev_salary
FROM departments d
INNER JOIN department_totals dt ON d.id = dt.dept_id
ORDER BY location, budget DESC;
