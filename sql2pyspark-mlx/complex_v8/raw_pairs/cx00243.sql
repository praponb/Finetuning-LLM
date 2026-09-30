SELECT student_id
FROM enrollments
EXCEPT ALL
SELECT id
FROM courses
ORDER BY student_id DESC
