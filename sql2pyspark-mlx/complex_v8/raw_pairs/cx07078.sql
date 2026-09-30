SELECT 'enrollment' AS origin, student_id, score AS metric
FROM enrollments
UNION ALL
SELECT 'student' AS origin, id AS student_id, gpa AS metric
FROM students
ORDER BY origin
