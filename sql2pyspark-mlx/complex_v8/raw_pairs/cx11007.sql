SELECT 'course' AS origin, credits, level AS label
FROM courses
UNION
SELECT 'student' AS origin, id, student_name
FROM students
WHERE enrolled_on >= DATE '2024-05-18'
ORDER BY credits DESC, origin
