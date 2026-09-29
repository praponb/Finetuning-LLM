SELECT id, term AS name
FROM enrollments
INTERSECT ALL
SELECT id, dept
FROM courses
WHERE LENGTH(TRIM(course_code)) >= 7
ORDER BY id
