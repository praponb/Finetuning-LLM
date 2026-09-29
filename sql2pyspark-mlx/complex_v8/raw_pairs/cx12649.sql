SELECT id, credits AS entity_id, dept
FROM courses
UNION ALL
SELECT student_id, id, grade_status
FROM enrollments
UNION ALL
SELECT credits, id, dept
FROM courses
WHERE credits < 4
ORDER BY id
