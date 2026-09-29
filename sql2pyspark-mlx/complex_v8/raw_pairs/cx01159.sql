WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level
    FROM courses
    WHERE credits > 2
)
SELECT id AS key_id
FROM base
WHERE level = 'intro'
  AND dept IN ('Art', 'Math', 'CS')
EXCEPT
SELECT id
FROM students
ORDER BY key_id
