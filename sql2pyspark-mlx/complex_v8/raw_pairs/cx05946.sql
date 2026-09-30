WITH valid_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international
    FROM students
    WHERE UPPER(TRIM(student_name)) LIKE 'I%'
      AND gpa >= 2.7
)
SELECT
    cohort,
    student_name,
    enrolled_on AS day,
    'student' AS source
FROM valid_students
UNION ALL
SELECT
    major,
    student_name,
    enrolled_on,
    'student_2' AS source
FROM students
