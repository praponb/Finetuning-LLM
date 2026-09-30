WITH base_students AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        gpa,
        is_international,
        COALESCE(cohort, 'UNKNOWN') AS cohort_or_unknown
    FROM students
    WHERE major = 'Math'
)
SELECT major AS category, enrolled_on AS day
FROM students
UNION ALL
SELECT cohort AS category, enrolled_on AS day
FROM students
