SELECT id, score
FROM enrollments
UNION ALL
SELECT id, gpa
FROM students
