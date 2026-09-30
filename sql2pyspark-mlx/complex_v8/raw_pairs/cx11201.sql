WITH prepared AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE course_id IS NOT NULL
)
SELECT grade_status AS name, id, 'enrollment' AS origin
FROM prepared
UNION ALL
SELECT cohort, id, 'student' AS origin
FROM students
WHERE is_international = TRUE
  AND UPPER(TRIM(student_name)) LIKE 'C%'
UNION ALL
SELECT grade_status, id, 'enrollment_3' AS origin
FROM enrollments
WHERE term = '2024T1'
  AND grade_status = 'FAIL'
