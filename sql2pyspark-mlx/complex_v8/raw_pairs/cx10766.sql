SELECT
    course_id,
    enrolled_at,
    id,
    'enrollment' AS source
FROM enrollments
WHERE term = '2025T2'
  AND student_id IS NOT NULL
UNION
SELECT
    student_id AS course_id,
    enrolled_at,
    id,
    'enrollment_2' AS source
FROM enrollments
WHERE grade_status IN ('WITHDRAWN', 'PASS', 'FAIL')
  AND TO_DATE(enrolled_at) < DATE '2025-02-18'
ORDER BY source, id
