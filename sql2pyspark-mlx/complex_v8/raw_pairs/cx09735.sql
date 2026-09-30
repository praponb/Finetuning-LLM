SELECT course_id AS qty, student_id AS entity_id
FROM enrollments
UNION ALL
SELECT course_id AS qty, id AS entity_id
FROM enrollments
UNION ALL
SELECT student_id AS qty, course_id AS entity_id
FROM enrollments
WHERE grade_status <> 'FAIL'
  AND MONTH(enrolled_at) IN (1, 3)
ORDER BY qty, entity_id
