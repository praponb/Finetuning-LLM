SELECT credits, dept AS category
FROM courses
WHERE credits >= 2
  AND dept IN ('Math', 'Biz')
INTERSECT
SELECT id AS credits, cohort AS category
FROM students
WHERE is_international = TRUE
  AND cohort NOT IN ('C2022', 'C2024')
ORDER BY credits DESC
