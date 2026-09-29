SELECT id AS key_id, student_name
FROM students
EXCEPT
SELECT credits, course_code
FROM courses
WHERE dept = 'Art'
ORDER BY student_name DESC, key_id
