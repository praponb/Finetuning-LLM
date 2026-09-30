WITH valid_employees AS (
    SELECT *
    FROM employees
    WHERE MONTH(hire_date) IN (1, 2, 8) AND manager_id IS NULL AND is_remote
),
Summary AS (
    SELECT level, CASE WHEN level IN ('L3', 'L4') THEN 'priority' ELSE 'other' END AS level_group, COUNT(*) AS employee_count
    FROM valid_employees
    GROUP BY level, CASE WHEN level IN ('L3', 'L4') THEN 'priority' ELSE 'other' END
)
SELECT level, level_group, employee_count
FROM Summary;
