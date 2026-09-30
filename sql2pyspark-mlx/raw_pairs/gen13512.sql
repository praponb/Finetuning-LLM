WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, SUBSTRING(location, 1, 3) AS location_prefix
    FROM departments
    WHERE location IS NOT NULL
)
-- Latest matching employee per row (LATERAL ... LIMIT 1)
SELECT
    s.id,
    s.location,
    s.budget,
    s.location_prefix,
    latest.employee_level
FROM base s
LEFT JOIN LATERAL (
        SELECT c.level AS employee_level
        FROM employees c
        WHERE c.dept_id = s.id
        ORDER BY c.hire_date DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE;
