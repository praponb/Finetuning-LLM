SELECT major, student_name AS category
FROM students
UNION
SELECT term AS major, grade_status AS category
FROM enrollments
WHERE enrolled_at >= TIMESTAMP '2025-02-06 10:00:00'
  AND student_id IS NOT NULL
ORDER BY category DESC
