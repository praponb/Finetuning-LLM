WITH kept AS (
    SELECT s.*
    FROM departments s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.salary > 149000
        )
)
SELECT
    id,
    dept_name,
    budget,
    CASE WHEN dept_name RLIKE '^(Engineering|Finance)$' THEN 'CORE' WHEN dept_name RLIKE '^(Sales|Marketing)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS dept_name_zone
FROM kept
ORDER BY dept_name_zone DESC NULLS LAST, dept_name;
