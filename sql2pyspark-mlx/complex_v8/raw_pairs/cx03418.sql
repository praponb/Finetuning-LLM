WITH prepared AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        credits,
        level
    FROM courses
    WHERE dept IN ('Biz', 'Art', 'CS')
)
SELECT course_code AS category
FROM prepared
INTERSECT ALL
SELECT course_code
FROM courses
ORDER BY category DESC
