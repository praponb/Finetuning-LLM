SELECT id AS qty, enrolled_at AS event_time
FROM enrollments
WHERE TO_DATE(enrolled_at) < DATE '2025-03-08'
  AND grade_status <> 'PASS'
UNION
SELECT student_id, enrolled_at
FROM enrollments
ORDER BY qty, event_time
