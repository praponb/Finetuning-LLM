WITH filtered_employees AS (
    SELECT *
    FROM employees
    WHERE hire_date >= '2023-04-04' AND dept_id = 24
),
level_summary AS (
    SELECT level, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values, AVG(salary) AS avg_salary
    FROM filtered_employees
    GROUP BY level
)
SELECT level, level_values, avg_salary
FROM level_summary
WHERE avg_salary < 99000
