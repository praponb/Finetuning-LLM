SELECT
    'enrollment' AS origin,
    id,
    enrolled_at,
    score
FROM enrollments
WHERE course_id IS NOT NULL
UNION ALL
SELECT
    'enrollment_2' AS origin,
    id,
    enrolled_at,
    score
FROM enrollments
WHERE grade_status IN ('INCOMPLETE', 'PASS')
  AND MONTH(enrolled_at) IN (2, 3)
ORDER BY score
