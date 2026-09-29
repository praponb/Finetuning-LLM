SELECT term AS category
FROM enrollments
WHERE score <= 73.7
  AND course_id IS NOT NULL
EXCEPT
SELECT grade_status
FROM enrollments
WHERE course_id >= 11
