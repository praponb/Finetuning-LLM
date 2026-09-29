WITH recent AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        gpa,
        is_international,
        CONCAT_WS(' / ', cohort, student_name) AS cohort_student_name_label
    FROM students
    WHERE gpa < 3.2
      AND NOT is_international
)
SELECT 'student' AS src, id AS key_id, cohort_student_name_label
FROM recent
UNION ALL
SELECT 'student_2' AS src, id, major
FROM students
WHERE student_name IS NOT NULL
UNION ALL
SELECT 'course' AS src, id, level
FROM courses
ORDER BY key_id DESC
