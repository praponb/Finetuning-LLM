SELECT id, score AS value, enrolled_at
FROM enrollments
UNION ALL
SELECT course_id AS id, score AS value, enrolled_at
FROM enrollments
WHERE course_id >= 6
  AND HOUR(enrolled_at) BETWEEN 8 AND 14
