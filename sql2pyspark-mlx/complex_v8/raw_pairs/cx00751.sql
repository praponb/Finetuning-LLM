SELECT id AS qty, student_name, gpa AS value
FROM students
WHERE is_international = TRUE
  AND enrolled_on BETWEEN DATE '2024-02-28' AND DATE '2025-07-18'
UNION ALL
SELECT id, grade_status, score
FROM enrollments
UNION ALL
SELECT course_id, grade_status, score
FROM enrollments
