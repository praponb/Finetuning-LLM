SELECT course_id AS ref_id, grade_status AS name
FROM enrollments
WHERE student_id <= 10
INTERSECT ALL
SELECT credits, course_code
FROM courses
WHERE credits > 2
  AND level = 'core'
ORDER BY name, ref_id
