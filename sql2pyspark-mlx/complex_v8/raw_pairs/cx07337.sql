SELECT term
FROM enrollments
WHERE enrolled_at < TIMESTAMP '2025-02-24 12:00:00'
  AND term IN ('2024T1', '2025T2', '2024T2')
EXCEPT
SELECT course_code AS term
FROM courses
WHERE dept IN ('Bio', 'Math')
ORDER BY term
