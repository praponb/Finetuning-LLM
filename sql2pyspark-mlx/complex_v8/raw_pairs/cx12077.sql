SELECT course_id, student_id
FROM enrollments
UNION ALL
SELECT credits AS course_id, id AS student_id
FROM courses
UNION ALL
SELECT student_id AS course_id, course_id AS student_id
FROM enrollments
WHERE student_id IS NOT NULL
ORDER BY student_id, course_id
