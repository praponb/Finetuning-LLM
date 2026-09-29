SELECT course_id AS ref_id, term AS name, enrolled_at AS event_time
FROM enrollments
WHERE score BETWEEN 32.7 AND 76.8
UNION
SELECT course_id AS ref_id, grade_status AS name, enrolled_at AS event_time
FROM enrollments
