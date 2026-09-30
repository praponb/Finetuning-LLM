WITH filtered_employees AS (
    SELECT *
    FROM employees
    WHERE manager_id IS NULL AND is_remote AND salary > 83000
),
agg AS (
    SELECT job_title, AVG(salary) AS avg_salary, SUM(CASE WHEN level = 'L2' THEN 1 ELSE 0 END) AS l2_count
    FROM filtered_employees
    GROUP BY job_title
)
SELECT *
FROM agg
WHERE l2_count >= 44
