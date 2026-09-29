SELECT id, student_name AS label
FROM students
WHERE LENGTH(TRIM(student_name)) >= 8
UNION ALL
SELECT id, term AS label
FROM enrollments
WHERE enrolled_at >= TIMESTAMP '2025-01-07 12:00:00'
  AND score > 38.1
