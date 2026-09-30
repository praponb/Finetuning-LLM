SELECT
    id,
    term,
    score,
    'enrollment' AS origin
FROM enrollments
WHERE MONTH(enrolled_at) IN (1, 2)
  AND student_id IS NOT NULL
UNION ALL
SELECT
    id,
    grade_status,
    score,
    'enrollment_2' AS origin
FROM enrollments
ORDER BY origin DESC
