SELECT id, major
FROM students
UNION ALL
SELECT course_id AS id, term AS major
FROM enrollments
UNION ALL
SELECT credits AS id, course_code AS major
FROM courses
