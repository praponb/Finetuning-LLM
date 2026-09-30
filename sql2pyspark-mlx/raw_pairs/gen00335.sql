WITH with_latest AS (
    SELECT
        s.id,
        s.dept_name,
        x.level
    FROM departments s
    LEFT JOIN LATERAL (
            SELECT c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    dept_name,
    COUNT(*) AS row_count,
    COUNT(*) FILTER (WHERE level = 'L4') AS l4_rows,
    COUNT(*) FILTER (WHERE dept_name = 'Marketing') AS marketing_rows
FROM with_latest
GROUP BY CUBE (dept_name)
ORDER BY marketing_rows DESC
