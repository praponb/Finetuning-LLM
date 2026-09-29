SELECT id, student_name
FROM students
WHERE gpa < 3.4
EXCEPT ALL
SELECT id, major AS student_name
FROM students
ORDER BY student_name DESC
