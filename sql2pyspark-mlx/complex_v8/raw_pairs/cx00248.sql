SELECT id, score, enrolled_at AS happened_at
FROM enrollments
UNION
SELECT student_id, score, enrolled_at
FROM enrollments
ORDER BY happened_at
