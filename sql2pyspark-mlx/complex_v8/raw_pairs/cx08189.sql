SELECT credits, dept AS name, 'course' AS source
FROM courses
UNION
SELECT id AS credits, major AS name, 'student' AS source
FROM students
WHERE cohort IN ('C2022', 'C2024', 'C2023')
  AND YEAR(enrolled_on) = 2025
ORDER BY source
