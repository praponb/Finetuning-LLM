WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        major,
        gpa,
        is_international
    FROM students
    WHERE enrolled_on IS NOT NULL
)
SELECT major
FROM scoped
INTERSECT
SELECT dept AS major
FROM courses
WHERE dept = 'Math'
ORDER BY major
