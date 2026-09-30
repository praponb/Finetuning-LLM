SELECT cohort
FROM students
EXCEPT
SELECT dept AS cohort
FROM courses
WHERE dept IS NOT NULL
ORDER BY cohort
