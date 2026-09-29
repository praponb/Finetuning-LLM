SELECT 'enrollment' AS src, term, score AS metric
FROM enrollments
UNION ALL
SELECT 'enrollment_2' AS src, grade_status, score
FROM enrollments
UNION ALL
SELECT 'enrollment_3' AS src, term, score
FROM enrollments
WHERE student_id IS NOT NULL
ORDER BY metric DESC, term
