SELECT level, course_code, id AS qty
FROM courses
UNION ALL
SELECT grade_status, term, id
FROM enrollments
WHERE HOUR(enrolled_at) BETWEEN 6 AND 11
ORDER BY level DESC
