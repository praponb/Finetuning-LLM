SELECT
    student_id AS qty,
    id AS key_id,
    enrolled_at AS happened_at,
    'enrollment' AS src
FROM enrollments
UNION ALL
SELECT
    student_id,
    course_id,
    enrolled_at,
    'enrollment_2' AS src
FROM enrollments
