SELECT
    student_id,
    enrolled_at,
    score,
    'enrollment' AS src
FROM enrollments
UNION ALL
SELECT
    student_id,
    enrolled_at,
    score,
    'enrollment_2' AS src
FROM enrollments
ORDER BY src DESC
