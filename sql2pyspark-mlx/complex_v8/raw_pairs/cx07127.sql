SELECT id, cohort AS name
FROM students
WHERE gpa BETWEEN 2.5 AND 3.2
  AND enrolled_on IS NOT NULL
INTERSECT ALL
SELECT course_id, term
FROM enrollments
WHERE course_id IS NOT NULL
