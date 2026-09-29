SELECT id, enrolled_on, 'student' AS src
FROM students
WHERE cohort IN ('C2022', 'C2023')
  AND is_international = TRUE
UNION ALL
SELECT id, enrolled_on, 'student_2' AS src
FROM students
WHERE enrolled_on BETWEEN DATE '2024-04-04' AND DATE '2025-06-27'
ORDER BY src DESC
