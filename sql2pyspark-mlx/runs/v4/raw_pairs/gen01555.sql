WITH base AS (
    SELECT *, CASE WHEN location IN ('London', 'Singapore') THEN 'Tier 1' ELSE 'Other' END AS location_group
    FROM departments
    WHERE location IS NOT NULL
)
SELECT s.*, top1.hire_date
FROM base s
LEFT JOIN LATERAL (
        SELECT c.hire_date
        FROM employees c
        WHERE c.dept_id = s.id
        ORDER BY c.salary DESC, c.id DESC
        LIMIT 1
    ) top1 ON TRUE;
