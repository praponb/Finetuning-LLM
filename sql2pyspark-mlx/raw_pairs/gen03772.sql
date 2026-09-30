WITH latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.location,
        latest.level
    FROM departments s
    JOIN LATERAL (
            SELECT c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN dept_name = 'HR' THEN 1 ELSE 0 END AS is_hr
    FROM latest_match
    WHERE dept_name IS NOT NULL
      AND dept_name IS DISTINCT FROM 'Finance'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.dept_id IN (15, 29, 31)
        )
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.manager_id IS NOT NULL
        )
)
-- Grouped metrics
SELECT
    dept_name,
    SUM(CASE WHEN level = 'L2' THEN is_hr ELSE 0 END) AS l2_is_hr,
    COUNT(*) FILTER (WHERE dept_name = 'Finance') AS finance_rows,
    ROUND(AVG(is_hr), 2) AS avg_is_hr,
    COUNT(*) AS row_count
FROM not_flagged
GROUP BY dept_name;
