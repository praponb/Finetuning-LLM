WITH valid AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        credits,
        level
    FROM courses
)
SELECT id AS qty
FROM valid
INTERSECT ALL
SELECT id AS qty
FROM students
WHERE YEAR(enrolled_on) = 2025
  AND LENGTH(TRIM(student_name)) >= 5
ORDER BY qty DESC
