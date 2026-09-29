WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        dept,
        credits,
        level,
        COALESCE(dept, 'UNKNOWN') AS dept_or_unknown
    FROM courses
    WHERE credits <= 3
      AND level IN ('core', 'advanced')
)
SELECT id AS entity_id, level
FROM scoped
UNION ALL
SELECT id AS entity_id, cohort AS level
FROM students
UNION ALL
SELECT id AS entity_id, student_name AS level
FROM students
WHERE is_international = FALSE
ORDER BY entity_id, level
