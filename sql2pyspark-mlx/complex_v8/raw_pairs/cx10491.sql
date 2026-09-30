WITH cleaned_students AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        is_international,
        LOWER(REGEXP_REPLACE(TRIM(cohort), ' ', '_')) AS cohort_slug
    FROM students
    WHERE LOWER(student_name) LIKE '%iz%'
      AND cohort <> 'C2023'
)
SELECT id
FROM cleaned_students
EXCEPT ALL
SELECT course_id AS id
FROM enrollments
ORDER BY id DESC
