SELECT term, id
FROM enrollments
WHERE student_id IS NOT NULL
UNION
SELECT term, course_id AS id
FROM enrollments
WHERE course_id <= 9
  AND score >= 34.2
ORDER BY term
