SELECT id AS qty
FROM courses
WHERE credits >= 2
EXCEPT
SELECT id AS qty
FROM enrollments
WHERE grade_status = 'INCOMPLETE'
  AND term = '2025T1'
ORDER BY qty
