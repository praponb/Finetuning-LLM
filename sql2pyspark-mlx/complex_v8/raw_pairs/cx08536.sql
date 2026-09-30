SELECT major AS category, gpa
FROM students
UNION ALL
SELECT student_name AS category, gpa
FROM students
ORDER BY gpa
