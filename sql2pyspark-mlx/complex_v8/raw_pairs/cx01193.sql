WITH filtered AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        credits,
        level
    FROM courses
    WHERE dept IN ('CS', 'Art')
)
SELECT id AS entity_id
FROM filtered
WHERE UPPER(TRIM(course_code)) LIKE 'M%'
  AND level <> 'advanced'
EXCEPT
SELECT id
FROM students
ORDER BY entity_id
