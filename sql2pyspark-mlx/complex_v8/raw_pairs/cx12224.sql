WITH prepared AS (
    -- Filter students
    SELECT
        id,
        major,
        gpa,
        is_international
    FROM students
    WHERE enrolled_on IS NOT NULL
      AND is_international = FALSE
)
SELECT
    id,
    enrolled_on,
    gpa AS metric,
    'student' AS source
FROM students
WHERE LENGTH(TRIM(student_name)) >= 5
UNION
SELECT
    id,
    enrolled_on,
    gpa,
    'student_2' AS source
FROM students
ORDER BY enrolled_on
