WITH deduped AS (
    SELECT id, dept_name
    FROM departments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) = 1
),
filtered AS (
    SELECT *, LOWER(dept_name) AS dept_name_lower
    FROM deduped
    WHERE dept_name IS NOT NULL
),
not_flagged AS (
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.is_remote
        )
),
filtered_departments AS (
    SELECT
        *,
        UPPER(dept_name) AS dept_name_upper,
        CASE WHEN dept_name = 'HR' THEN 1 ELSE 0 END AS is_hr,
        COALESCE(dept_name, 'NONE') AS resolved_dept_name
    FROM not_flagged
    WHERE dept_name = 'HR'
)
SELECT
    dept_name,
    CASE WHEN is_hr >= 74 THEN 'LARGE' ELSE 'SMALL' END AS is_hr_tier,
    COUNT(CASE WHEN dept_name = 'Engineering' THEN 1 END) AS engineering_count,
    COUNT(*) FILTER (WHERE dept_name = 'Legal') AS legal_rows,
    COUNT(*) AS row_count,
    RANK() OVER (PARTITION BY dept_name ORDER BY COUNT(*) DESC) AS row_count_rank
FROM filtered_departments
GROUP BY dept_name, CASE WHEN is_hr >= 74 THEN 'LARGE' ELSE 'SMALL' END;
