WITH recent_courses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        credits,
        level
    FROM courses
)
SELECT course_code AS name
FROM recent_courses
WHERE credits <= 3
EXCEPT
SELECT student_name
FROM students
ORDER BY name DESC
