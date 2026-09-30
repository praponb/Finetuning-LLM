SELECT grade_status AS label, enrolled_at AS event_time
FROM enrollments
UNION ALL
SELECT term AS label, enrolled_at AS event_time
FROM enrollments
WHERE course_id >= 4
  AND enrolled_at < TIMESTAMP '2025-02-12 09:00:00'
UNION ALL
SELECT term AS label, enrolled_at AS event_time
FROM enrollments
WHERE HOUR(enrolled_at) BETWEEN 6 AND 12
  AND term = '2024T1'
