SELECT
    'enrollment' AS origin,
    id,
    score,
    grade_status
FROM enrollments
UNION ALL
SELECT
    'enrollment_2' AS origin,
    student_id,
    score,
    term
FROM enrollments
ORDER BY id
