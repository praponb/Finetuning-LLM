SELECT term AS label, score AS value, course_id AS ref_id
FROM enrollments
UNION
SELECT cohort, gpa, id
FROM students
WHERE major = 'Art'
  AND gpa > 2.7
