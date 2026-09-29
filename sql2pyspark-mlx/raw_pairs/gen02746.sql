WITH ValidEmployees AS (
    SELECT *
    FROM employees
    WHERE is_remote AND dept_id IN (4, 7) AND salary < 66000
),
agg AS (
    SELECT manager_id, ROUND(STDDEV(salary), 2) AS stddev_salary
    FROM ValidEmployees
    GROUP BY manager_id
)
SELECT *
FROM agg
