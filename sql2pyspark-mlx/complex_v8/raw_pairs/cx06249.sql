SELECT student_id AS entity_id, grade_status AS label
FROM enrollments
WHERE grade_status = 'FAIL'
UNION ALL
SELECT id AS entity_id, student_name AS label
FROM students
WHERE gpa > 2.7
UNION ALL
SELECT id AS entity_id, term AS label
FROM enrollments
WHERE term = '2024T1'
  AND HOUR(enrolled_at) BETWEEN 6 AND 10
ORDER BY label DESC, entity_id
