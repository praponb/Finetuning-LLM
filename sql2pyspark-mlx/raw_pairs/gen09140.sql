WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.level = 'L5'
        )
)
SELECT
    id,
    dept_name,
    location,
    budget,
    CASE WHEN dept_name RLIKE '^(Finance|Engineering)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS dept_name_zone
FROM eligible
ORDER BY budget NULLS FIRST;
