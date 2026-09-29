SELECT id, enrolled_at
FROM enrollments
UNION ALL
SELECT course_id, enrolled_at
FROM enrollments
UNION ALL
SELECT id, enrolled_at
FROM enrollments
