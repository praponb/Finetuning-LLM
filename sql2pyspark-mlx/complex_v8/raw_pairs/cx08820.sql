SELECT id AS qty, student_id
FROM enrollments
UNION ALL
SELECT id, credits
FROM courses
ORDER BY student_id DESC
