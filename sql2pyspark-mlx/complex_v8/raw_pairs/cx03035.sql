SELECT course_id, term
FROM enrollments
WHERE score > 13.9
EXCEPT
SELECT id, student_name
FROM students
WHERE major IS NOT NULL
