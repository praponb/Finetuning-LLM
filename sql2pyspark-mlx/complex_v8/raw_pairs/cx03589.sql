SELECT
    course_id,
    term AS name,
    enrolled_at,
    'enrollment' AS record_type
FROM enrollments
UNION
SELECT
    student_id,
    grade_status,
    enrolled_at,
    'enrollment_2' AS record_type
FROM enrollments
ORDER BY record_type, name
