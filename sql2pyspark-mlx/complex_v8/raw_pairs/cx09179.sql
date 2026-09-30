WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE score <= 59.8
)
SELECT grade_status AS label
FROM prepared_enrollments
WHERE grade_status_len < 10
  AND course_id IS NOT NULL
EXCEPT
SELECT term
FROM enrollments
WHERE course_id >= 5
ORDER BY label
