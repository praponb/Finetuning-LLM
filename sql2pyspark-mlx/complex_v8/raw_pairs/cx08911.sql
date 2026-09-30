SELECT 'enrollment' AS src, id, student_id
FROM enrollments
WHERE TO_DATE(enrolled_at) < DATE '2025-02-06'
  AND grade_status IN ('FAIL', 'WITHDRAWN')
UNION ALL
SELECT 'enrollment_2' AS src, id, course_id
FROM enrollments
UNION ALL
SELECT 'course' AS src, credits, id
FROM courses
