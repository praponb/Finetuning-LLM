SELECT major AS category, enrolled_on AS event_date
FROM students
WHERE gpa >= 2.0
  AND is_international = TRUE
UNION ALL
SELECT major, enrolled_on
FROM students
UNION ALL
SELECT cohort, enrolled_on
FROM students
WHERE cohort = 'C2021'
ORDER BY event_date DESC, category
