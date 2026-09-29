SELECT id, enrolled_at
FROM enrollments
UNION ALL
SELECT student_id, enrolled_at
FROM enrollments
ORDER BY id DESC
