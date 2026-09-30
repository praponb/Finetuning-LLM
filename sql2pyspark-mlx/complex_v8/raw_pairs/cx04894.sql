SELECT student_id AS ref_id, grade_status AS name, enrolled_at
FROM enrollments
UNION
SELECT course_id AS ref_id, term AS name, enrolled_at
FROM enrollments
WHERE score <= 73.7
ORDER BY name DESC
