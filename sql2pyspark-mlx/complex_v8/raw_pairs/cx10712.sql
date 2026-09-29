SELECT 'student' AS source, id AS entity_id, enrolled_on
FROM students
UNION ALL
SELECT 'student_2' AS source, id AS entity_id, enrolled_on
FROM students
WHERE gpa > 2.0
  AND major = 'Art'
ORDER BY enrolled_on DESC, source
