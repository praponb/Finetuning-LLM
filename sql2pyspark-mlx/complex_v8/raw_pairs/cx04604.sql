SELECT course_id, score AS value
FROM enrollments
UNION
SELECT id, score
FROM enrollments
WHERE grade_status <> 'PASS'
