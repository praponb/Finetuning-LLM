SELECT credits, course_code
FROM courses
WHERE credits < 3
  AND UPPER(TRIM(course_code)) LIKE 'B%'
UNION ALL
SELECT student_id, grade_status
FROM enrollments
WHERE student_id IS NOT NULL
UNION ALL
SELECT id, major
FROM students
WHERE gpa >= 2.7
  AND enrolled_on IS NOT NULL
ORDER BY course_code DESC
