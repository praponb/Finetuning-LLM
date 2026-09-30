SELECT id AS qty, enrolled_at, grade_status AS name
FROM enrollments
UNION ALL
SELECT course_id, enrolled_at, grade_status
FROM enrollments
ORDER BY name
