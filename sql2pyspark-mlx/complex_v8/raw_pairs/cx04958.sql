SELECT grade_status AS category, term
FROM enrollments
WHERE term = '2025T1'
  AND grade_status = 'FAIL'
UNION ALL
SELECT student_name, major
FROM students
WHERE cohort = 'C2022'
  AND is_international = TRUE
UNION ALL
SELECT major, student_name
FROM students
WHERE NOT is_international
  AND gpa < 3.4
ORDER BY category
