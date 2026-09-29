SELECT term AS label, score AS value
FROM enrollments
UNION
SELECT grade_status AS label, score AS value
FROM enrollments
WHERE score >= 30.0
  AND MONTH(enrolled_at) IN (1, 3)
