SELECT
    term AS name,
    enrolled_at,
    score AS amount,
    'enrollment' AS origin
FROM enrollments
UNION ALL
SELECT
    grade_status,
    enrolled_at,
    score,
    'enrollment_2' AS origin
FROM enrollments
WHERE student_id IS NOT NULL
