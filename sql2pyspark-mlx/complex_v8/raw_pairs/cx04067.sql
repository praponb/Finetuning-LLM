SELECT student_name AS category, gpa
FROM students
WHERE major IN ('Art', 'Math')
  AND NOT is_international
UNION ALL
SELECT cohort AS category, gpa
FROM students
WHERE is_international = TRUE
UNION ALL
SELECT term AS category, score AS gpa
FROM enrollments
WHERE course_id >= 6
ORDER BY category DESC
