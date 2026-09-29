WITH department_stats AS (
    SELECT dept_id, ROUND(STDDEV(salary), 2) AS stddev_salary, SUM(CASE WHEN level = 'L5' THEN 1 ELSE 0 END) AS l5_count
    FROM employees
    GROUP BY dept_id
)
SELECT d.budget, ds.stddev_salary, ds.l5_count
FROM departments d
LEFT JOIN department_stats ds ON d.id = ds.dept_id
WHERE ds.l5_count > 37
ORDER BY budget;
