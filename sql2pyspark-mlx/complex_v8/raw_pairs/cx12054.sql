SELECT major, enrolled_on
FROM students
WHERE is_international = TRUE
  AND enrolled_on BETWEEN DATE '2024-03-15' AND DATE '2025-05-07'
UNION ALL
SELECT cohort AS major, enrolled_on
FROM students
