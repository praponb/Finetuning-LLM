SELECT id AS ref_id, student_name
FROM students
EXCEPT ALL
SELECT credits AS ref_id, course_code AS student_name
FROM courses
WHERE dept IS NOT NULL
ORDER BY student_name
