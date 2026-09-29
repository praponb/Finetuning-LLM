SELECT student_name
FROM students
WHERE is_international = TRUE
INTERSECT
SELECT course_code AS student_name
FROM courses
WHERE course_code IS NOT NULL
  AND dept IN ('Biz', 'CS')
